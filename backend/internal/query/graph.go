package query

import (
	"context"
	"sort"

	"golang.org/x/sync/errgroup"
)

// Graph is the scenario of the bot built from FSM transitions (Botsight.md, 13.2, 16.1).
// The empty state "" is the entry (no state yet) and the exit (state cleared). Bots have
// loops ("start over"), so this is a directed graph, not a Sankey diagram.
type Graph struct {
	Nodes   []Node  `json:"nodes"`
	Edges   []Edge  `json:"edges"`
	Retries []Retry `json:"retries"`
}

type Edge struct {
	From        string  `json:"from"`
	To          string  `json:"to"`
	Transitions uint64  `json:"transitions"`
	Users       uint64  `json:"users"`
	Share       float64 `json:"share"` // of the users who left the From state by any edge
}

type Node struct {
	State string `json:"state"`
	In    uint64 `json:"in"` // users entering, summed over edges
	Out   uint64 `json:"out"`
}

// Retry: the handler kept the state, i.e. the user is asked the same thing again
// (Botsight.md, 13.1). Neutral handlers like /help do not count.
type Retry struct {
	State   string `json:"state"`
	Retries uint64 `json:"retries"`
	Users   uint64 `json:"users"`
}

const maxEdges = 500

func graphQueries(projectID uint32, rg Range, neutral []string) (edges, retries Query) {
	p := Params{}
	where := scope(p, projectID, rg)
	if neutral == nil {
		neutral = []string{}
	}
	edges = Query{p, `
SELECT state_from, state_to, count() AS transitions, uniqExact(user_id) AS users
FROM events
WHERE ` + where + ` AND type = 'step' AND error = '' AND state_from != state_to
GROUP BY state_from, state_to
ORDER BY users DESC, state_from, state_to
LIMIT ` + p.P("max_edges", "UInt32", uint32(maxEdges))}
	retries = Query{p, `
SELECT state_from AS state, count() AS retries, uniqExact(user_id) AS users
FROM events
WHERE ` + where + ` AND type = 'step' AND error = '' AND state_from != '' AND state_from = state_to
  AND handler NOT IN ` + p.P("neutral", "Array(String)", neutral) + `
GROUP BY state
ORDER BY retries DESC, state
LIMIT 100`}
	return
}

func (r *Runner) Graph(ctx context.Context, projectID uint32, rg Range, neutral []string) (Graph, bool, error) {
	return cached(ctx, r, "graph", []any{projectID, rg, neutral}, func(ctx context.Context) (Graph, error) {
		qe, qr := graphQueries(projectID, rg, neutral)
		var out Graph
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			rows, err := r.query(ctx, qe)
			if err != nil {
				return err
			}
			out.Edges, err = scanAll(rows, func(e *Edge) []any { return []any{&e.From, &e.To, &e.Transitions, &e.Users} })
			return err
		})
		g.Go(func() error {
			rows, err := r.query(ctx, qr)
			if err != nil {
				return err
			}
			out.Retries, err = scanAll(rows, func(v *Retry) []any { return []any{&v.State, &v.Retries, &v.Users} })
			return err
		})
		if err := g.Wait(); err != nil {
			return Graph{}, err
		}
		out.Nodes = nodes(out.Edges)
		return out, nil
	})
}

// nodes sums the edges around every state and fills the share of each edge.
func nodes(edges []Edge) []Node {
	byState := map[string]*Node{}
	node := func(s string) *Node {
		if n, ok := byState[s]; ok {
			return n
		}
		n := &Node{State: s}
		byState[s] = n
		return n
	}
	for _, e := range edges {
		node(e.From).Out += e.Users
		node(e.To).In += e.Users
	}
	for i := range edges {
		edges[i].Share = share(edges[i].Users, byState[edges[i].From].Out)
	}
	out := make([]Node, 0, len(byState))
	for _, n := range byState {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool {
		if a, b := out[i].In+out[i].Out, out[j].In+out[j].Out; a != b {
			return a > b
		}
		return out[i].State < out[j].State
	})
	return out
}
