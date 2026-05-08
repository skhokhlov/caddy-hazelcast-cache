package hazelcast

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/darkweak/storages/core"
	hzclient "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/types"
	"github.com/pierrec/lz4/v4"
)

// ErrNotInitialised is returned by write paths invoked before Init has
// connected the underlying client. Read paths return nil instead, matching
// the core.Storer convention that Get is total.
var ErrNotInitialised = errors.New("hazelcast: provider not initialised")

// providerName is the storer identifier exposed to Souin via Name(). Kept as
// a private constant so callers go through Name() rather than referencing the
// string directly.
const providerName = "HAZELCAST"

// mapAPI is the subset of *hazelcast.Map this package consumes. Defined as an
// interface so unit tests can substitute an in-memory fake while integration
// tests exercise the real client. *hazelcast.Map satisfies it implicitly.
type mapAPI interface {
	Get(ctx context.Context, key any) (any, error)
	Set(ctx context.Context, key, value any) error
	SetWithTTL(ctx context.Context, key, value any, ttl time.Duration) error
	Remove(ctx context.Context, key any) (any, error)
	Lock(ctx context.Context, key any) error
	Unlock(ctx context.Context, key any) error
	GetKeySet(ctx context.Context) ([]any, error)
	GetEntrySet(ctx context.Context) ([]types.Entry, error)
}

// hzClient is the subset of *hzclient.Client we hold for shutdown. The real
// client satisfies it directly.
type hzClient interface {
	Shutdown(ctx context.Context) error
}

// connector opens a Hazelcast client and resolves the configured IMap. Tests
// override this field; production uses defaultConnector.
type connector func(ctx context.Context, cfg *Config, log logger.Logger) (hzClient, mapAPI, error)

func defaultConnector(ctx context.Context, cfg *Config, log logger.Logger) (hzClient, mapAPI, error) {
	client, err := newClient(ctx, cfg, log)
	if err != nil {
		return nil, nil, err
	}
	imap, err := client.GetMap(ctx, cfg.MapName)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Shutdown(shutdownCtx)
		return nil, nil, fmt.Errorf("hazelcast: getting map %q: %w", cfg.MapName, err)
	}
	return client, imap, nil
}

// instanceRegistry tracks live providers keyed by Uuid so a Caddy reload that
// produces an equivalent provider does not leak the underlying client. The
// pattern mirrors the badger provider in darkweak/storages.
var instanceRegistry sync.Map

// Hazelcast is the core.Storer implementation backed by a Hazelcast IMap.
//
// The zero value is unusable; construct via New, then call Init exactly once
// before reading or writing. Reset releases the underlying client and removes
// the provider from the instance registry; a subsequent Init reconnects.
type Hazelcast struct {
	cfg    *Config
	stale  time.Duration
	logger logger.Logger
	uuid   string

	connector connector

	mu     sync.Mutex
	client hzClient
	imap   mapAPI
}

// New builds a provider from validated config. The provider is unconnected
// until Init is called.
func New(cfg *Config, log logger.Logger, stale time.Duration) (*Hazelcast, error) {
	if cfg == nil {
		return nil, ErrNilConfig
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Hazelcast{
		cfg:       cfg,
		stale:     stale,
		logger:    log,
		uuid:      computeUuid(cfg, stale),
		connector: defaultConnector,
	}, nil
}

// Name returns the storer identifier used by Souin's registry.
func (h *Hazelcast) Name() string { return providerName }

// Uuid returns a deterministic identifier derived from cluster name, map name
// and the configured stale window. Two providers with identical configuration
// share a Uuid and therefore the same instance-registry slot.
func (h *Hazelcast) Uuid() string { return h.uuid }

// Init connects the underlying Hazelcast client and registers this provider
// in the instance registry. It is idempotent: a second call on an already
// connected provider returns nil without reconnecting.
func (h *Hazelcast) Init() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.imap != nil {
		return nil
	}
	ctx, cancel := initContext(h.cfg)
	defer cancel()
	client, imap, err := h.connector(ctx, h.cfg, h.logger)
	if err != nil {
		return err
	}
	h.client = client
	h.imap = imap
	instanceRegistry.Store(h.uuid, h)
	return nil
}

// Reset closes the underlying Hazelcast client and removes the provider from
// the instance registry. After Reset, a subsequent Init reconnects.
func (h *Hazelcast) Reset() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	client := h.client
	h.client = nil
	h.imap = nil
	instanceRegistry.Delete(h.uuid)
	if client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		return fmt.Errorf("hazelcast: shutting down client: %w", err)
	}
	return nil
}

// Get returns the bytes stored at key, or nil on miss, on transport error or
// before Init. core.Storer.Get is total (no error return); failures degrade
// to a miss so the caller path stays tight.
func (h *Hazelcast) Get(key string) []byte {
	imap := h.activeMap()
	if imap == nil {
		return nil
	}
	ctx, cancel := h.opContext(h.cfg.ReadTimeout)
	defer cancel()
	raw, err := imap.Get(ctx, key)
	if err != nil || raw == nil {
		return nil
	}
	if b, ok := raw.([]byte); ok {
		return b
	}
	return nil
}

// Set stores value at key. The effective TTL is duration + the provider's
// stale window, matching the convention used by the badger provider so the
// shared MappingElection / revalidation logic in storages/core sees fresh +
// stale entries with consistent expirations.
func (h *Hazelcast) Set(key string, value []byte, duration time.Duration) error {
	imap := h.activeMap()
	if imap == nil {
		return ErrNotInitialised
	}
	ctx, cancel := h.opContext(h.cfg.WriteTimeout)
	defer cancel()
	if err := imap.SetWithTTL(ctx, key, value, duration+h.stale); err != nil {
		return fmt.Errorf("hazelcast: set %q: %w", key, err)
	}
	return nil
}

// Delete removes a single key. core.Storer.Delete has no error return; any
// upstream failure is swallowed (the caller has no way to act on it and the
// next eviction or TTL will reclaim the entry).
func (h *Hazelcast) Delete(key string) {
	imap := h.activeMap()
	if imap == nil {
		return
	}
	ctx, cancel := h.opContext(h.cfg.WriteTimeout)
	defer cancel()
	_, _ = imap.Remove(ctx, key)
}

// DeleteMany removes every key whose name matches the supplied Go regular
// expression. The v1 strategy is a full key-set scan; the Phase 4.2
// benchmark gate decides whether to swap in Hazelcast SQL `DELETE … WHERE
// __key LIKE …` past 100k keys. core.Storer.DeleteMany has no error return,
// so an invalid pattern is treated as "matches nothing" and silently
// ignored.
func (h *Hazelcast) DeleteMany(pattern string) {
	imap := h.activeMap()
	if imap == nil {
		return
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return
	}
	ctx, cancel := h.opContext(h.cfg.WriteTimeout)
	defer cancel()
	keys, err := imap.GetKeySet(ctx)
	if err != nil {
		return
	}
	for _, k := range keys {
		s, ok := k.(string)
		if !ok {
			continue
		}
		if re.MatchString(s) {
			_, _ = imap.Remove(ctx, s)
		}
	}
}

// MapKeys returns every entry whose key starts with prefix, with the prefix
// stripped. Values are returned as strings; the caller is expected to know
// whether the underlying bytes are textual (Souin treats mapping protobuf
// bytes opaquely here, only feeding them back to DecodeMapping when needed).
func (h *Hazelcast) MapKeys(prefix string) map[string]string {
	out := map[string]string{}
	imap := h.activeMap()
	if imap == nil {
		return out
	}
	ctx, cancel := h.opContext(h.cfg.ReadTimeout)
	defer cancel()
	entries, err := imap.GetEntrySet(ctx)
	if err != nil {
		return out
	}
	for _, e := range entries {
		k, ok := e.Key.(string)
		if !ok || !strings.HasPrefix(k, prefix) {
			continue
		}
		b, ok := e.Value.([]byte)
		if !ok {
			continue
		}
		out[strings.TrimPrefix(k, prefix)] = string(b)
	}
	return out
}

// ListKeys walks the mapping index (IDX_* entries), decodes each protobuf
// mapping and returns the RealKey of every variation it carries. Keys that
// fail to decode are skipped — they are either stale or written by a future
// schema and a subsequent SetMultiLevel will overwrite them.
func (h *Hazelcast) ListKeys() []string {
	imap := h.activeMap()
	if imap == nil {
		return nil
	}
	ctx, cancel := h.opContext(h.cfg.ReadTimeout)
	defer cancel()
	entries, err := imap.GetEntrySet(ctx)
	if err != nil {
		return nil
	}
	var keys []string
	for _, e := range entries {
		k, ok := e.Key.(string)
		if !ok || !strings.HasPrefix(k, core.MappingKeyPrefix) {
			continue
		}
		b, ok := e.Value.([]byte)
		if !ok {
			continue
		}
		mapper, err := core.DecodeMapping(b)
		if err != nil || mapper == nil {
			continue
		}
		for _, idx := range mapper.GetMapping() {
			if rk := idx.GetRealKey(); rk != "" {
				keys = append(keys, rk)
			}
		}
	}
	return keys
}

// GetMultiLevel fetches the IDX_<key> mapping bytes and delegates to
// core.MappingElection, which performs Vary matching, ETag revalidation and
// the actual body read (via h.Get) for the elected variation.
//
// Returns (nil, nil) if the mapping is missing, the IMap is unreachable, or
// no variation matches; (fresh, nil) for a hit; (nil, stale) for a stale hit.
func (h *Hazelcast) GetMultiLevel(key string, req *http.Request, validator *core.Revalidator) (*http.Response, *http.Response) {
	imap := h.activeMap()
	if imap == nil {
		return nil, nil
	}
	ctx, cancel := h.opContext(h.cfg.ReadTimeout)
	defer cancel()
	raw, err := imap.Get(ctx, core.MappingKeyPrefix+key)
	if err != nil || raw == nil {
		return nil, nil
	}
	b, ok := raw.([]byte)
	if !ok {
		return nil, nil
	}
	fresh, stale, _ := core.MappingElection(h, b, req, validator, noopCoreLogger{})
	return fresh, stale
}

// SetMultiLevel stores a varied response body (lz4-compressed) and updates
// the IDX_<baseKey> mapping protobuf so subsequent GetMultiLevel calls can
// elect the right variation by Vary headers and ETag.
//
// Hazelcast IMaps offer no multi-key transaction; the read-modify-write of
// the mapping is therefore guarded with imap.Lock(IDX_<baseKey>) so 20
// concurrent SetMultiLevel calls on the same baseKey converge to a mapping
// containing all 20 variations rather than racing into a lost-update.
func (h *Hazelcast) SetMultiLevel(baseKey, variedKey string, value []byte, variedHeaders http.Header, etag string, duration time.Duration, realKey string) error {
	imap := h.activeMap()
	if imap == nil {
		return ErrNotInitialised
	}
	compressed, err := lz4Compress(value)
	if err != nil {
		return fmt.Errorf("hazelcast: lz4 compress %q: %w", variedKey, err)
	}

	ttl := duration + h.stale
	ctx, cancel := h.opContext(h.cfg.WriteTimeout)
	defer cancel()

	if err := imap.SetWithTTL(ctx, variedKey, compressed, ttl); err != nil {
		return fmt.Errorf("hazelcast: set varied %q: %w", variedKey, err)
	}

	mappingKey := core.MappingKeyPrefix + baseKey
	if err := imap.Lock(ctx, mappingKey); err != nil {
		return fmt.Errorf("hazelcast: lock %q: %w", mappingKey, err)
	}
	defer func() {
		unlockCtx, cancelUnlock := h.opContext(h.cfg.WriteTimeout)
		defer cancelUnlock()
		_ = imap.Unlock(unlockCtx, mappingKey)
	}()

	var existing []byte
	if raw, err := imap.Get(ctx, mappingKey); err == nil && raw != nil {
		if b, ok := raw.([]byte); ok {
			existing = b
		}
	}

	now := time.Now()
	updated, err := core.MappingUpdater(
		variedKey,
		existing,
		noopCoreLogger{},
		now,
		now.Add(duration),
		now.Add(duration+h.stale),
		variedHeaders,
		etag,
		realKey,
	)
	if err != nil {
		return fmt.Errorf("hazelcast: update mapping for %q: %w", baseKey, err)
	}
	if err := imap.SetWithTTL(ctx, mappingKey, updated, ttl); err != nil {
		return fmt.Errorf("hazelcast: set mapping %q: %w", mappingKey, err)
	}
	return nil
}

func lz4Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func lz4Decompress(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (h *Hazelcast) activeMap() mapAPI {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.imap
}

func (h *Hazelcast) opContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), timeout)
}

func initContext(cfg *Config) (context.Context, context.CancelFunc) {
	timeout := cfg.ConnectTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

func computeUuid(cfg *Config, stale time.Duration) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%d", cfg.ClusterName, cfg.MapName, int64(stale))
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// lookupInstance returns the registered provider for a given Uuid, or nil if
// no provider is currently registered.
func lookupInstance(uuid string) *Hazelcast {
	v, ok := instanceRegistry.Load(uuid)
	if !ok {
		return nil
	}
	return v.(*Hazelcast)
}

// Compile-time assertions: *hzclient.Client satisfies hzClient, and the
// Hazelcast provider satisfies the full core.Storer interface.
var (
	_ hzClient    = (*hzclient.Client)(nil)
	_ core.Storer = (*Hazelcast)(nil)
)
