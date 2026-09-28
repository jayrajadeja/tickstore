package store

import (
	"os"
	"sync"

	"github.com/jayrajadeja/tickstore/tick"
)

// Cache is a resident, read-only view over a Store's logs. It keeps, per symbol,
// the open log fd plus its parsed sparse index and record count hot in memory, so
// repeat Range/Last calls skip the per-request open + full index ReadFile that the
// stateless Store pays. It is validated on every call by a cheap Stat: if the log
// grew, shrank, or was replaced, the fd and index are reloaded. Correctness is
// identical to Store (the same rangeFrom/lastFrom core runs over the hot fd).
//
// Safe for concurrent use. The read itself runs while the RWMutex is held (a read
// lock on the fast path, the write lock on a reload), so a reload can never close
// an fd that a reader is still using. *os.File.ReadAt is offset-free and a built
// index slice is never mutated, so many readers share the hot state under RLock.
type Cache struct {
	s  *Store
	mu sync.RWMutex
	m  map[string]*hot
}

// hot is the cached state for one symbol's log.
type hot struct {
	f       *os.File
	size    int64
	modUnix int64
	count   int64
	entries []indexEntry
}

// NewCached returns a Cache over the store rooted at dir.
func NewCached(dir string) *Cache {
	return &Cache{s: New(dir), m: make(map[string]*hot)}
}

var _ Reader = (*Cache)(nil)

// Range returns all ticks with from <= TS <= to (logical time), ascending,
// mirroring Store.Range but served from the resident fd + index.
func (c *Cache) Range(symbol string, from, to int64) ([]tick.Tick, error) {
	if from > to {
		return nil, nil
	}
	return c.read(symbol, func(h *hot) ([]tick.Tick, error) {
		return rangeFrom(h.f, h.count, h.entries, from, to)
	})
}

// Last returns the final n ticks (fewer if the log is shorter), ascending.
func (c *Cache) Last(symbol string, n int) ([]tick.Tick, error) {
	if n <= 0 {
		return nil, nil
	}
	return c.read(symbol, func(h *hot) ([]tick.Tick, error) {
		return lastFrom(h.f, h.count, n)
	})
}

// read resolves the hot state for symbol and runs fn on it while the relevant
// lock is held, so the fd cannot be closed by a concurrent reload mid-read. A
// missing log skips fn and returns an empty result.
func (c *Cache) read(symbol string, fn func(h *hot) ([]tick.Tick, error)) ([]tick.Tick, error) {
	// Fast path: hot entry that is still fresh — serve under the read lock.
	c.mu.RLock()
	h := c.m[symbol]
	if h != nil {
		fresh, err := stillFresh(h)
		if err != nil {
			c.mu.RUnlock()
			return nil, err
		}
		if fresh {
			defer c.mu.RUnlock()
			return fn(h)
		}
	}
	c.mu.RUnlock()

	// Slow path: (re)load under the write lock, then serve while still holding it.
	c.mu.Lock()
	defer c.mu.Unlock()
	// Another goroutine may have reloaded between the two locks.
	if h := c.m[symbol]; h != nil {
		fresh, err := stillFresh(h)
		if err != nil {
			return nil, err
		}
		if fresh {
			return fn(h)
		}
	}
	h, err := c.load(symbol)
	if err != nil || h == nil {
		return nil, err
	}
	return fn(h)
}

// stillFresh reports whether the cached fd's log is unchanged (same size and
// mtime). A vanished file is treated as not fresh so load can re-resolve it.
func stillFresh(h *hot) (bool, error) {
	info, err := h.f.Stat()
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.Size() == h.size && info.ModTime().Unix() == h.modUnix, nil
}

// load opens the log, parses its index, and caches the state. The caller holds
// the write lock, so closing any previous fd here is safe — no reader can hold it.
// A missing log removes any stale entry and returns (nil, nil).
func (c *Cache) load(symbol string) (*hot, error) {
	if old := c.m[symbol]; old != nil {
		old.f.Close()
		delete(c.m, symbol)
	}
	f, err := c.s.openForRead(symbol)
	if err != nil || f == nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	count, _ := recordCount(info.Size())
	entries, _ := loadIndex(c.s.idxPath(symbol)) // bad/missing index simply falls back
	h := &hot{
		f:       f,
		size:    info.Size(),
		modUnix: info.ModTime().Unix(),
		count:   count,
		entries: entries,
	}
	c.m[symbol] = h
	return h, nil
}

// Close releases all cached file descriptors. After Close the Cache must not be
// used. Intended for graceful shutdown.
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for sym, h := range c.m {
		if err := h.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(c.m, sym)
	}
	return firstErr
}
