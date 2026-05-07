// Package memory is an in-memory storage.Backend for tests. Maps a string
// path to a byte slice; Open returns a File backed by bytes.Reader.
//
// Useful when integration-testing the rfile reader against synthetic
// RFile bytes without touching disk or GCS. Production code should not
// import this package.
package memory

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/accumulo/shoal/internal/storage"
)

// Backend is an in-memory map[path]bytes. Concurrent-safe Put/Open.
type Backend struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// New returns an empty Backend ready for Put.
func New() *Backend {
	return &Backend{objects: map[string][]byte{}}
}

// Put registers data at path. Subsequent Open calls for path will return
// a File reading those bytes. Replaces any existing entry.
//
// Stores a defensive copy so callers can mutate data afterwards without
// affecting the registered fixture.
func (b *Backend) Put(path string, data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[path] = cp
}

// Delete removes path. No-op if absent.
func (b *Backend) Delete(path string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, path)
}

// Open returns a File reading bytes registered at path. Returns
// storage.ErrNotFound if no such path is registered.
func (b *Backend) Open(_ context.Context, path string) (storage.File, error) {
	b.mu.RLock()
	data, ok := b.objects[path]
	b.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, path)
	}
	return &file{r: bytes.NewReader(data), size: int64(len(data))}, nil
}

// Create returns a Writer that, on Close, registers its accumulated
// bytes at path (replacing any prior value). Until Close, Open(path)
// still sees the old bytes — the new entry is only published atomically
// at flush time, mirroring most cloud-storage write semantics.
func (b *Backend) Create(_ context.Context, path string) (storage.Writer, error) {
	return &writer{b: b, path: path, buf: &bytes.Buffer{}}, nil
}

// writer is the memory-backend's Writer. Buffers writes; on Close
// publishes the buffered bytes to b.objects[path].
type writer struct {
	b      *Backend
	path   string
	buf    *bytes.Buffer
	closed bool
}

func (w *writer) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("memory: write after close")
	}
	return w.buf.Write(p)
}

func (w *writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.b.Put(w.path, w.buf.Bytes())
	return nil
}

// file wraps bytes.Reader as a storage.File. bytes.Reader already
// satisfies io.ReaderAt; we add Close (no-op) and Size.
type file struct {
	r    *bytes.Reader
	size int64
}

func (m *file) ReadAt(p []byte, off int64) (int, error) { return m.r.ReadAt(p, off) }
func (m *file) Close() error                            { return nil }
func (m *file) Size() int64                             { return m.size }
