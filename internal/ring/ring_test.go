package ring

import (
	"strconv"
	"testing"
)

func testRing(t *testing.T, vnodes int, ids ...string) *Ring {
	t.Helper()
	r := New(vnodes)
	for _, id := range ids {
		r.Add(id)
	}
	return r
}

func TestOwnersDeterministic(t *testing.T) {
	r := testRing(t, 150, "a", "b", "c")
	first := r.Owners("user:42", 2)
	for i := 0; i < 100; i++ {
		got := r.Owners("user:42", 2)
		if len(got) != 2 || got[0] != first[0] || got[1] != first[1] {
			t.Fatalf("iteration %d: got %v want %v", i, got, first)
		}
	}
}

func TestOwnersUniqueAndCapped(t *testing.T) {
	r := testRing(t, 150, "a", "b", "c")
	got := r.Owners("k", 2)
	if len(got) != 2 {
		t.Fatalf("R=2: got %v", got)
	}
	if got[0] == got[1] {
		t.Fatalf("duplicate owners: %v", got)
	}
	all := r.Owners("k", 99)
	if len(all) != 3 {
		t.Fatalf("R>N should cap at N, got %v", all)
	}
	seen := map[string]bool{}
	for _, id := range all {
		if seen[id] {
			t.Fatalf("duplicate in full walk: %v", all)
		}
		seen[id] = true
	}
}

func TestEmptyAndZeroReplicas(t *testing.T) {
	r := New(10)
	if got := r.Owners("x", 2); got != nil {
		t.Fatalf("empty ring: %v", got)
	}
	r.Add("a")
	if got := r.Owners("x", 0); got != nil {
		t.Fatalf("n=0: %v", got)
	}
	if r.Primary("x") != "a" {
		t.Fatalf("single node primary: %q", r.Primary("x"))
	}
}

func TestAddRemoveIdempotent(t *testing.T) {
	r := New(8)
	r.Add("a")
	r.Add("a")
	if r.Size() != 1 || r.Len() != 8 {
		t.Fatalf("size=%d len=%d", r.Size(), r.Len())
	}
	r.Remove("missing")
	r.Remove("a")
	r.Remove("a")
	if r.Size() != 0 || r.Len() != 0 {
		t.Fatalf("after remove size=%d len=%d", r.Size(), r.Len())
	}
}

func TestVirtualNodeCount(t *testing.T) {
	r := testRing(t, 150, "a", "b", "c")
	if r.Len() != 450 {
		t.Fatalf("virtual nodes: got %d want 450", r.Len())
	}
	if r.VNodesPerNode() != 150 {
		t.Fatalf("vnodes per node: %d", r.VNodesPerNode())
	}
	if VNodeName("a", 3) != "a#3" {
		t.Fatalf("vnode name formula changed: %q", VNodeName("a", 3))
	}
}

func TestKeySpreadThreeNodes(t *testing.T) {
	r := testRing(t, 150, "node-a", "node-b", "node-c")
	counts := map[string]int{}
	const n = 10000
	for i := 0; i < n; i++ {
		p := r.Primary("k:" + strconv.Itoa(i))
		counts[p]++
	}
	for _, id := range []string{"node-a", "node-b", "node-c"} {
		// With 150 vnodes, a 10k-key sample should stay well off 0% / 100%.
		if counts[id] < n/10 {
			t.Fatalf("node %s only got %d/%d keys (vnode imbalance)", id, counts[id], n)
		}
	}
}

func TestJoinRemapsAboutOneNth(t *testing.T) {
	r := testRing(t, 150, "a", "b", "c")
	const n = 5000
	before := make([]string, n)
	for i := 0; i < n; i++ {
		before[i] = r.Primary("k:" + strconv.Itoa(i))
	}
	r.Add("d")
	changed := 0
	for i := 0; i < n; i++ {
		if r.Primary("k:"+strconv.Itoa(i)) != before[i] {
			changed++
		}
	}
	frac := float64(changed) / float64(n)
	// Consistent hashing: ~1/4 of primaries should move when 3 → 4 nodes.
	if frac < 0.12 || frac > 0.40 {
		t.Fatalf("remap fraction %.3f outside [0.12, 0.40] (changed=%d)", frac, changed)
	}
}

func TestLeaveRemapsAboutOneNth(t *testing.T) {
	r := testRing(t, 150, "a", "b", "c", "d")
	const n = 5000
	before := make([]string, n)
	for i := 0; i < n; i++ {
		before[i] = r.Primary("k:" + strconv.Itoa(i))
	}
	r.Remove("d")
	changed := 0
	for i := 0; i < n; i++ {
		if r.Primary("k:"+strconv.Itoa(i)) != before[i] {
			changed++
		}
	}
	frac := float64(changed) / float64(n)
	// 4 → 3: keys whose primary was d (~1/4) should move.
	if frac < 0.12 || frac > 0.40 {
		t.Fatalf("leave remap fraction %.3f outside [0.12, 0.40] (changed=%d)", frac, changed)
	}
}

func TestReplicaSetStableAfterUnrelatedJoin(t *testing.T) {
	r := testRing(t, 150, "a", "b")
	owners := r.Owners("sticky-key", 2)
	if len(owners) != 2 {
		t.Fatalf("owners: %v", owners)
	}
	r.Add("c")
	// The key may remapped, but Owners still returns 2 unique live nodes.
	got := r.Owners("sticky-key", 2)
	if len(got) != 2 || got[0] == got[1] {
		t.Fatalf("after join: %v", got)
	}
}

func TestNodesSorted(t *testing.T) {
	r := testRing(t, 4, "c", "a", "b")
	got := r.Nodes()
	want := []string{"a", "b", "c"}
	if len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("got %v want %v", got, want)
	}
}
