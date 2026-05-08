package hazelcast

import (
	"context"
	"sync"
	"time"

	"github.com/hazelcast/hazelcast-go-client/types"
)

// fakeMap is an in-memory mapAPI used by unit tests. It stores []byte values
// keyed by string and respects SetWithTTL expirations using a synthetic clock
// based on time.Now (sufficient for unit-test latencies).
type fakeMap struct {
	mu      sync.Mutex
	data    map[string]fakeEntry
	getErr  error
	setErr  error
	rmErr   error
	lockSeq []string // records lock/unlock pairs for assertions
}

type fakeEntry struct {
	value   []byte
	expires time.Time
}

func newFakeMap() *fakeMap {
	return &fakeMap{data: map[string]fakeEntry{}}
}

func (f *fakeMap) snapshot() map[string][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string][]byte, len(f.data))
	for k, v := range f.data {
		out[k] = append([]byte(nil), v.value...)
	}
	return out
}

func (f *fakeMap) Get(_ context.Context, key any) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	e, ok := f.data[key.(string)]
	if !ok {
		return nil, nil
	}
	if !e.expires.IsZero() && time.Now().After(e.expires) {
		delete(f.data, key.(string))
		return nil, nil
	}
	return e.value, nil
}

func (f *fakeMap) Set(_ context.Context, key, value any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setErr != nil {
		return f.setErr
	}
	f.data[key.(string)] = fakeEntry{value: copyBytes(value)}
	return nil
}

func (f *fakeMap) SetWithTTL(_ context.Context, key, value any, ttl time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setErr != nil {
		return f.setErr
	}
	e := fakeEntry{value: copyBytes(value)}
	if ttl > 0 {
		e.expires = time.Now().Add(ttl)
	}
	f.data[key.(string)] = e
	return nil
}

func (f *fakeMap) Remove(_ context.Context, key any) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rmErr != nil {
		return nil, f.rmErr
	}
	k := key.(string)
	e, ok := f.data[k]
	if !ok {
		return nil, nil
	}
	delete(f.data, k)
	return e.value, nil
}

func (f *fakeMap) Lock(_ context.Context, key any) error {
	f.mu.Lock()
	f.lockSeq = append(f.lockSeq, "L:"+key.(string))
	f.mu.Unlock()
	return nil
}

func (f *fakeMap) Unlock(_ context.Context, key any) error {
	f.mu.Lock()
	f.lockSeq = append(f.lockSeq, "U:"+key.(string))
	f.mu.Unlock()
	return nil
}

func (f *fakeMap) GetKeySet(context.Context) ([]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]any, 0, len(f.data))
	for k := range f.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func (f *fakeMap) GetEntrySet(context.Context) ([]types.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entries := make([]types.Entry, 0, len(f.data))
	for k, v := range f.data {
		entries = append(entries, types.Entry{Key: k, Value: append([]byte(nil), v.value...)})
	}
	return entries, nil
}

func copyBytes(value any) []byte {
	b, ok := value.([]byte)
	if !ok {
		return nil
	}
	return append([]byte(nil), b...)
}

// fakeClient is an in-memory hzClient that records Shutdown invocations.
type fakeClient struct {
	mu        sync.Mutex
	shutdowns int
	shutErr   error
}

func (f *fakeClient) Shutdown(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shutdowns++
	return f.shutErr
}

func (f *fakeClient) shutdownCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shutdowns
}
