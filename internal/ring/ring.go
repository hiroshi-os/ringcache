// Package ring implements a consistent-hash ring with virtual nodes.
//
// # Placement
//
// Each physical node is mapped onto the ring V times (virtual nodes). Virtual
// node i of physical node id is addressed as:
//
//	hash32(id + "#" + strconv.Itoa(i))   // FNV-1a 32-bit
//
// A key is placed on the first unique physical nodes encountered walking
// clockwise from hash32(key). Replica owners are that walk truncated to R
// distinct nodes (or the whole cluster if R > N).
//
// # Why virtual nodes
//
// A single hash per physical node produces large, uneven arcs. V placements
// per node (default 150) split those arcs so key load is closer to 1/N and a
// join/leave remaps roughly 1/N of keys instead of a whole adjacent slice.
package ring

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// vnode is one point on the hash ring.
type vnode struct {
	hash uint32
	id   string
}

// Ring is a thread-safe consistent hash ring.
type Ring struct {
	mu            sync.RWMutex
	vnodesPerNode int
	nodes         map[string]struct{}
	ring          []vnode
}

// New returns an empty ring. vnodesPerNode must be >= 1.
func New(vnodesPerNode int) *Ring {
	if vnodesPerNode < 1 {
		vnodesPerNode = 1
	}
	return &Ring{
		vnodesPerNode: vnodesPerNode,
		nodes:         make(map[string]struct{}),
	}
}

// VNodesPerNode returns the configured virtual-node factor.
func (r *Ring) VNodesPerNode() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.vnodesPerNode
}

// Add places a physical node onto the ring. Idempotent.
func (r *Ring) Add(id string) {
	if id == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[id]; ok {
		return
	}
	r.nodes[id] = struct{}{}
	r.rebuildLocked()
}

// Remove takes a physical node off the ring. Idempotent.
func (r *Ring) Remove(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[id]; !ok {
		return
	}
	delete(r.nodes, id)
	r.rebuildLocked()
}

// Nodes returns physical node IDs in sorted order.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Size is the number of physical nodes.
func (r *Ring) Size() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// Len is the number of virtual nodes currently on the ring.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.ring)
}

// Owners returns up to n distinct physical nodes responsible for key,
// walking clockwise from hash(key). n is capped at the cluster size.
func (r *Ring) Owners(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n < 1 || len(r.ring) == 0 || len(r.nodes) == 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}
	h := Hash32(key)
	i := sort.Search(len(r.ring), func(i int) bool { return r.ring[i].hash >= h })
	if i == len(r.ring) {
		i = 0
	}
	seen := make(map[string]struct{}, n)
	out := make([]string, 0, n)
	for walked := 0; walked < len(r.ring) && len(out) < n; walked++ {
		id := r.ring[i].id
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			out = append(out, id)
		}
		i++
		if i == len(r.ring) {
			i = 0
		}
	}
	return out
}

// Primary is Owners(key, 1) or "" if the ring is empty.
func (r *Ring) Primary(key string) string {
	o := r.Owners(key, 1)
	if len(o) == 0 {
		return ""
	}
	return o[0]
}

// Hash32 is the ring's key/vnode hash (FNV-1a 32-bit). Exported for tests.
func Hash32(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// VNodeName is the documented virtual-node identity: id#i
func VNodeName(id string, i int) string {
	return id + "#" + strconv.Itoa(i)
}

func (r *Ring) rebuildLocked() {
	ring := make([]vnode, 0, len(r.nodes)*r.vnodesPerNode)
	for id := range r.nodes {
		for i := 0; i < r.vnodesPerNode; i++ {
			ring = append(ring, vnode{
				hash: Hash32(VNodeName(id, i)),
				id:   id,
			})
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		if ring[i].hash == ring[j].hash {
			return ring[i].id < ring[j].id
		}
		return ring[i].hash < ring[j].hash
	})
	r.ring = ring
}

// Describe returns a human-readable snapshot for /ring.
func (r *Ring) Describe() string {
	return fmt.Sprintf("physical=%d virtual=%d vnodes_per_node=%d", r.Size(), r.Len(), r.VNodesPerNode())
}
