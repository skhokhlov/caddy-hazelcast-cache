package hazelcast

import (
	"context"
	"sync"
	"time"

	"github.com/hazelcast/hazelcast-go-client/types"
)

// fakeMap is an in-memory mapAPI used by unit tests. It honours SetWithTTL
// expirations using a real-time clock (sufficient at unit-test latencies) and
// can inject transport errors via getErr / setErr / rmErr.
type fakeMap struct {
	mu     sync.Mutex
	data   map[string]fakeEntry
	getErr error
	setErr error
	rmErr  error
}

type fakeEntry struct {
	value   []byte
	expires time.Time
}

func newFakeMap() *fakeMap {
	return &fakeMap{data: map[string]fakeEntry{}}
}

func (f *fakeMap) entryFor(key string) (fakeEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, ok := f.data[key]
	return entry, ok
}

func (f *fakeMap) Get(_ context.Context, key any) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return nil, f.getErr
	}
	entry, ok := f.data[key.(string)]
	if !ok {
		return nil, nil
	}
	if !entry.expires.IsZero() && !time.Now().Before(entry.expires) {
		delete(f.data, key.(string))
		return nil, nil
	}
	return entry.value, nil
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
	entry := fakeEntry{value: copyBytes(value)}
	if ttl > 0 {
		entry.expires = time.Now().Add(ttl)
	}
	f.data[key.(string)] = entry
	return nil
}

func (f *fakeMap) Remove(_ context.Context, key any) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rmErr != nil {
		return nil, f.rmErr
	}
	k := key.(string)
	entry, ok := f.data[k]
	if !ok {
		return nil, nil
	}
	delete(f.data, k)
	return entry.value, nil
}

func (f *fakeMap) Lock(context.Context, any) error   { return nil }
func (f *fakeMap) Unlock(context.Context, any) error { return nil }

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

// fakeClient is an in-memory hzClient that records Shutdown invocations and
// can inject a shutdown error via err.
type fakeClient struct {
	mu        sync.Mutex
	shutdowns int
	err       error
}

func (f *fakeClient) Shutdown(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shutdowns++
	return f.err
}

func (f *fakeClient) shutdownCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shutdowns
}
