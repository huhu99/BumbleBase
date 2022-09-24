package concurrency

import (
	"errors"
	"sync"
)

// Graph.
type Graph struct {
	edges []Edge
	lock  sync.RWMutex
}

// Edge.
type Edge struct {
	from *Transaction
	to   *Transaction
}

// Grab a write lock on the graph
func (g *Graph) WLock() {
	g.lock.Lock()
}

// Release the write lock on the graph
func (g *Graph) WUnlock() {
	g.lock.Unlock()
}

// Grab a read lock on the graph
func (g *Graph) RLock() {
	g.lock.RLock()
}

// Release the write lock on the graph
func (g *Graph) RUnlock() {
	g.lock.RUnlock()
}

// Construct a new graph.
func NewGraph() *Graph {
	return &Graph{edges: make([]Edge, 0)}
}

// Add an edge from `from` to `to`. Logically, `from` waits for `to`.
func (g *Graph) AddEdge(from *Transaction, to *Transaction) {
	g.WLock()
	defer g.WUnlock()
	g.edges = append(g.edges, Edge{from: from, to: to})
}

// Remove an edge. Only removes one of these edges if multiple copies exist.
func (g *Graph) RemoveEdge(from *Transaction, to *Transaction) error {
	g.WLock()
	defer g.WUnlock()
	toRemove := Edge{from: from, to: to}
	for i, e := range g.edges {
		if e == toRemove {
			g.edges = removeEdge(g.edges, i)
			return nil
		}
	}
	return errors.New("edge not found")
}

// Return true if a cycle exists; false otherwise.
func (g *Graph) DetectCycle() bool {
	g.RLock()
	defer g.RUnlock()
	if len(g.edges) == 0 {
		return false
	}
	return dfs(g, g.edges[0].from, make([]*Transaction, 0))
}

func dfs(g *Graph, from *Transaction, seen []*Transaction) bool {
	// Go through each edge.
	for _, e := range g.edges {
		// If there is an edge from here to elsewhere,
		if e.from == from {
			// Check if it creates a cycle.
			for _, s := range seen {
				if e.to == s {
					return true
				}
			}
			// Otherwise, run dfs on it.
			return dfs(g, e.to, append(seen, e.from))
		}
	}
	return false
}

// Remove the element at index `i` from `l`.
func removeEdge(l []Edge, i int) []Edge {
	l[i] = l[len(l)-1]
	return l[:len(l)-1]
}
