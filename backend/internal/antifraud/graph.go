package antifraud

// Referral graph (Botsight.md, 15.1): a /start label ref_<user_id> is an edge
// "invited by". Connected components are found with union-find; a component is a ring
// when it is large and made of suspicious accounts, or is a wide star of accounts
// without usernames.

type unionFind struct {
	parent map[uint64]uint64
	size   map[uint64]int
}

func newUnionFind() *unionFind {
	return &unionFind{parent: map[uint64]uint64{}, size: map[uint64]int{}}
}

func (u *unionFind) find(x uint64) uint64 {
	p, ok := u.parent[x]
	if !ok {
		u.parent[x] = x
		u.size[x] = 1
		return x
	}
	if p == x {
		return x
	}
	root := u.find(p)
	u.parent[x] = root // path compression
	return root
}

func (u *unionFind) union(a, b uint64) {
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return
	}
	if u.size[ra] < u.size[rb] { // union by size keeps trees shallow
		ra, rb = rb, ra
	}
	u.parent[rb] = ra
	u.size[ra] += u.size[rb]
}

// Ring thresholds.
const (
	ringMinSize        = 10
	ringSuspiciousRate = 0.5
	starMinFanout      = 30
	starNoUsernameRate = 0.7
)

type edge struct{ invitee, inviter uint64 }

// member is what the graph job knows about a user from user_features.
type member struct {
	suspicious  bool // fast or synchronous: behavioural signals only
	hasUsername bool
	known       bool
}

// rings returns, for every user in a ring component, the component size.
func rings(edges []edge, members map[uint64]member) map[uint64]int {
	uf := newUnionFind()
	fanout := map[uint64]int{}
	for _, e := range edges {
		uf.union(e.invitee, e.inviter)
		fanout[e.inviter]++
	}
	type stats struct {
		size, known, suspicious, noUsername, maxFanout int
	}
	comps := map[uint64]*stats{}
	for node := range uf.parent {
		root := uf.find(node)
		st := comps[root]
		if st == nil {
			st = &stats{}
			comps[root] = st
		}
		st.size++
		st.maxFanout = max(st.maxFanout, fanout[node])
		if m, ok := members[node]; ok && m.known {
			st.known++
			if m.suspicious {
				st.suspicious++
			}
			if !m.hasUsername {
				st.noUsername++
			}
		}
	}
	out := map[uint64]int{}
	for node := range uf.parent {
		st := comps[uf.find(node)]
		if st.size < ringMinSize || st.known == 0 {
			continue
		}
		suspicious := float64(st.suspicious)/float64(st.known) >= ringSuspiciousRate
		star := st.maxFanout >= starMinFanout && float64(st.noUsername)/float64(st.known) >= starNoUsernameRate
		if suspicious || star {
			out[node] = st.size
		}
	}
	return out
}
