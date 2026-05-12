package hazelcast

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	defaultReadTimeout  = 5 * time.Second
	defaultWriteTimeout = 5 * time.Second
)

// errNotInitialised is returned by mutating storage methods when the provider
// has not been Init'd yet. Kept internal so the package's public surface
// remains the one named by the implementation plan.
var errNotInitialised = errors.New("hazelcast: provider not initialised")

// Get returns the bytes stored under key, or nil if the key is absent, the
// provider has not been initialised, the read times out, or the stored value
// is not a byte slice. core.Storer.Get is total (no error return), so
// failures degrade to a miss.
func (h *Hazelcast) Get(key string) []byte {
	imap := h.activeMap()
	if imap == nil {
		return nil
	}
	ctx, cancel := h.opContext(h.cfg.ReadTimeout, defaultReadTimeout)
	defer cancel()
	v, err := imap.Get(ctx, key)
	if err != nil || v == nil {
		return nil
	}
	b, ok := v.([]byte)
	if !ok {
		return nil
	}
	return b
}

// Set writes value under key with an effective TTL of duration + the
// provider's stale window. The padding matches the badger provider's
// convention so storages/core's MappingElection observes consistent fresh +
// stale windows.
func (h *Hazelcast) Set(key string, value []byte, duration time.Duration) error {
	imap := h.activeMap()
	if imap == nil {
		return errNotInitialised
	}
	ctx, cancel := h.opContext(h.cfg.WriteTimeout, defaultWriteTimeout)
	defer cancel()
	if err := imap.SetWithTTL(ctx, key, value, duration+h.stale); err != nil {
		return fmt.Errorf("hazelcast: set %q: %w", key, err)
	}
	return nil
}

// Delete removes a single key. core.Storer.Delete has no error return; any
// upstream failure is swallowed because the caller cannot act on it and the
// next eviction or TTL will reclaim the entry.
func (h *Hazelcast) Delete(key string) {
	imap := h.activeMap()
	if imap == nil {
		return
	}
	ctx, cancel := h.opContext(h.cfg.WriteTimeout, defaultWriteTimeout)
	defer cancel()
	_, _ = imap.Remove(ctx, key)
}

// DeleteMany removes every key whose name matches the supplied Go regular
// expression. The v1 strategy is a full key-set scan; the Phase 4.2
// benchmark gate decides whether to swap in Hazelcast SQL DELETE for keys
// with cardinality past 100k. core.Storer.DeleteMany has no error return,
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
	ctx, cancel := h.opContext(h.cfg.WriteTimeout, defaultWriteTimeout)
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

func (h *Hazelcast) activeMap() mapAPI {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.imap
}

func (h *Hazelcast) opContext(configured, fallback time.Duration) (context.Context, context.CancelFunc) {
	timeout := configured
	if timeout <= 0 {
		timeout = fallback
	}
	return context.WithTimeout(context.Background(), timeout)
}
