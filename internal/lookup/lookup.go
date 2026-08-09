package lookup

import (
	"sync"

	"github.com/phero20/concurrent-resource-scheduler/errors"
	"github.com/phero20/concurrent-resource-scheduler/internal/node"
)

// Map is a thread-safe registry mapping application keys to their canonical HeapNodes.
// It strictly synchronizes map access and must never trigger a Heap operation directly.
type Map[T any, ID comparable] struct {
	mu    sync.RWMutex
	nodes map[ID]*node.HeapNode[T, ID]
}

// New creates and returns a new empty Lookup Map.
func New[T any, ID comparable]() *Map[T, ID] {
	return &Map[T, ID]{
		nodes: make(map[ID]*node.HeapNode[T, ID]),
	}
}

// Add registers a new HeapNode. It returns ErrDuplicateKey if the key is already registered.
func (m *Map[T, ID]) Add(key ID, n *node.HeapNode[T, ID]) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.nodes[key]; exists {
		return errors.ErrDuplicateKey
	}
	m.nodes[key] = n
	return nil
}

// BatchAdd registers multiple HeapNodes atomically.
// It returns ErrDuplicateKey if any key is already registered, leaving the map unchanged.
func (m *Map[T, ID]) BatchAdd(nodes map[ID]*node.HeapNode[T, ID]) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Phase 1: Validate all
	for k := range nodes {
		if _, exists := m.nodes[k]; exists {
			return errors.ErrDuplicateKey
		}
	}

	// Phase 2: Insert all
	for k, n := range nodes {
		m.nodes[k] = n
	}
	return nil
}

// Get returns the registered HeapNode for the given key, or nil if not found.
func (m *Map[T, ID]) Get(key ID) *node.HeapNode[T, ID] {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodes[key]
}

// Remove deletes the HeapNode associated with the given key.
// It is a no-op if the key does not exist.
func (m *Map[T, ID]) Remove(key ID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, key)
}

// Len returns the total number of registered nodes in O(1) time.
func (m *Map[T, ID]) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes)
}
