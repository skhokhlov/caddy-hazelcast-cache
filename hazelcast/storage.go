package hazelcast

import (
	"context"
	"errors"
	"fmt"
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
