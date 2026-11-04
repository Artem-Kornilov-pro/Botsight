package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/live"
)

// Live stream of a project's events as Server-Sent Events (Botsight.md, 13.3, 16.2).
// The browser's EventSource reconnects by itself; there is no replay, a stream starts
// from now.

const (
	codeLiveOff        = "live_unavailable"
	codeTooManyStreams = "too_many_streams"
	liveHeartbeat      = 15 * time.Second
	liveWriteTimeout   = 10 * time.Second
)

func (s *Server) live(w http.ResponseWriter, r *http.Request) {
	if s.Live == nil {
		writeError(w, http.StatusServiceUnavailable, codeLiveOff, "live events are not configured")
		return
	}
	pid := uint32(projectFrom(r).ID)
	sub, err := s.Live.Subscribe(pid)
	if errors.Is(err, live.ErrTooMany) {
		w.Header().Set("Retry-After", "30")
		writeError(w, http.StatusTooManyRequests, codeTooManyStreams, "too many open live streams")
		return
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, codeLiveOff, "live events are unavailable")
		return
	}
	defer sub.Close()

	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no") // nginx would hold the stream in its buffer
	w.WriteHeader(http.StatusOK)

	// The server's WriteTimeout (30 s) would cut the stream: every write gets its own
	// deadline instead, so a dead client is still dropped within liveWriteTimeout.
	send := func(name, id string, data any) bool {
		_ = rc.SetWriteDeadline(time.Now().Add(liveWriteTimeout))
		b, err := json.Marshal(data)
		if err != nil {
			return false
		}
		if id != "" {
			_, err = fmt.Fprintf(w, "id: %s\n", id)
		}
		if err == nil {
			_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, b)
		}
		return err == nil && rc.Flush() == nil
	}
	reportDropped := func() bool {
		if n := sub.TakeDropped(); n > 0 {
			return send("dropped", "", map[string]int64{"count": n})
		}
		return true
	}

	if !send("ready", "", map[string]any{"project_id": pid, "heartbeat_seconds": int(liveHeartbeat.Seconds())}) {
		return
	}
	beat := time.NewTicker(liveHeartbeat)
	defer beat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-sub.C:
			if !ok { // the server is shutting down
				return
			}
			if !reportDropped() || !send("event", ev.EventID, ev) {
				return
			}
		case <-beat.C:
			// a comment keeps proxies and the browser from closing an idle connection
			_ = rc.SetWriteDeadline(time.Now().Add(liveWriteTimeout))
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil || rc.Flush() != nil || !reportDropped() {
				return
			}
		}
	}
}
