package hazelcast

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	hzclient "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/logger"
	"github.com/hazelcast/hazelcast-go-client/types"
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

// Compile-time assertion that *hzclient.Client satisfies hzClient.
var _ hzClient = (*hzclient.Client)(nil)
