package flight

import (
	"fmt"
	"sync"
)

// ClientPool maintains a pool of reusable Flight Client connections keyed by address.
// Connections are created lazily on first use and reused by subsequent callers.
// Thread-safe via double-checked locking.
type ClientPool struct {
	mu      sync.RWMutex
	clients map[string]*Client
	opts    []ClientOption
}

// NewClientPool creates a new ClientPool with optional shared ClientOptions
// applied to every connection created by the pool.
func NewClientPool(opts ...ClientOption) *ClientPool {
	return &ClientPool{
		clients: make(map[string]*Client),
		opts:    opts,
	}
}

// Get returns a Client for the given address. If no connection exists yet,
// a new one is created and stored. The returned Client is safe for concurrent use.
func (p *ClientPool) Get(addr string) (*Client, error) {
	// Fast path: read lock
	p.mu.RLock()
	c, ok := p.clients[addr]
	p.mu.RUnlock()
	if ok {
		return c, nil
	}

	// Slow path: create under write lock with double-check
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, ok = p.clients[addr]; ok {
		return c, nil
	}

	c, err := NewClient(addr, p.opts...)
	if err != nil {
		return nil, fmt.Errorf("client pool dial %s: %w", addr, err)
	}

	p.clients[addr] = c
	return c, nil
}

// Len returns the number of active connections in the pool.
func (p *ClientPool) Len() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients)
}

// Close closes all connections in the pool. Call during graceful shutdown.
func (p *ClientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for addr, c := range p.clients {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", addr, err)
		}
		delete(p.clients, addr)
	}
	return firstErr
}
