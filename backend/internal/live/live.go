// Package live streams the events of a project to the dashboard as they arrive
// (Botsight.md, 13.3, 16.2): the "waiting for the first event" screen and the live feed.
//
// Every api instance reads events.raw without a consumer group: a subscriber may be on any
// instance, so each must see all events. The consumer runs only while someone watches,
// starts at the end of the topic and decodes only records of watched projects, which it
// finds by the project_id header without parsing JSON.
package live

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
)

// Source delivers events.raw records from the end of the topic until ctx ends.
type Source interface {
	Run(ctx context.Context, deliver func(projectID uint32, value []byte)) error
}

var (
	ErrTooMany = errors.New("too many live streams")
	ErrClosed  = errors.New("live hub is closed")
)

// Event is what the dashboard shows of an event. User ids are strings: pseudonymized
// ids exceed 2^53, the largest integer of JavaScript.
type Event struct {
	EventID      string    `json:"event_id"`
	TS           time.Time `json:"ts"`
	Type         string    `json:"type"`
	Name         string    `json:"name"`
	UserID       string    `json:"user_id,omitempty"`
	ChatID       int64     `json:"chat_id,omitempty"`
	ContentType  string    `json:"content_type,omitempty"`
	Command      string    `json:"command,omitempty"`
	StartParam   string    `json:"start_param,omitempty"`
	CallbackData string    `json:"callback_data,omitempty"`
	State        string    `json:"state,omitempty"`
	Handler      string    `json:"handler,omitempty"`
	StateFrom    string    `json:"state_from,omitempty"`
	StateTo      string    `json:"state_to,omitempty"`
	MemberStatus string    `json:"member_status,omitempty"`
	Handled      bool      `json:"handled,omitempty"`
	OK           bool      `json:"ok,omitempty"`
	DurationMS   uint32    `json:"duration_ms,omitempty"`
	ErrorCode    uint16    `json:"error_code,omitempty"`
	Error        string    `json:"error,omitempty"`
}

func view(r event.Record) Event {
	e := Event{
		EventID: r.EventID, TS: time.UnixMilli(r.TS).UTC(), Type: r.Type, Name: r.Name, ChatID: r.ChatID,
		ContentType: r.ContentType, Command: r.Command, StartParam: r.StartParam, CallbackData: r.CallbackData,
		State: r.State, Handler: r.Handler, StateFrom: r.StateFrom, StateTo: r.StateTo, MemberStatus: r.MemberStatus,
		Handled: r.Handled, OK: r.OK, DurationMS: r.DurationMS, ErrorCode: r.ErrorCode, Error: r.Error,
	}
	if r.UserID != 0 {
		e.UserID = strconv.FormatUint(r.UserID, 10)
	}
	return e
}

// Hub fans events out to subscribers. A slow subscriber never holds the others back:
// its buffer fills, further events for it are dropped and counted, and the stream tells
// the dashboard how many it missed.
type Hub struct {
	source        Source
	logger        *slog.Logger
	Buffer        int           // events per subscriber, default 256
	MaxPerProject int           // default 20 (tabs of the team)
	MaxTotal      int           // per instance, default 2000
	Linger        time.Duration // the consumer outlives the last subscriber, against churn; default 30 s

	mu     sync.RWMutex
	subs   map[uint32]map[*Sub]struct{}
	total  int
	gen    uint64             // generation of the running consumer
	cancel context.CancelFunc // of the running consumer; nil when stopped
	idle   *time.Timer
	closed bool
	wg     sync.WaitGroup
}

func NewHub(source Source, logger *slog.Logger) *Hub {
	return &Hub{
		source: source, logger: logger, Buffer: 256, MaxPerProject: 20, MaxTotal: 2000, Linger: 30 * time.Second,
		subs: map[uint32]map[*Sub]struct{}{},
	}
}

// Sub is one stream. C is closed when the hub shuts down.
type Sub struct {
	C       <-chan Event
	ch      chan Event
	project uint32
	hub     *Hub
	dropped atomic.Int64
	once    sync.Once
}

// TakeDropped returns the number of events missed since the last call.
func (s *Sub) TakeDropped() int64 { return s.dropped.Swap(0) }

// Close ends the subscription; the consumer stops after Linger without subscribers.
func (s *Sub) Close() { s.once.Do(func() { s.hub.remove(s) }) }

// Streams is the number of open subscriptions, for the metrics.
func (h *Hub) Streams() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.total
}

// Running reports whether the consumer reads the topic, for tests and readiness.
func (h *Hub) Running() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.cancel != nil
}

func (h *Hub) Subscribe(projectID uint32) (*Sub, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	if h.total >= h.MaxTotal || len(h.subs[projectID]) >= h.MaxPerProject {
		return nil, ErrTooMany
	}
	ch := make(chan Event, h.Buffer)
	s := &Sub{C: ch, ch: ch, project: projectID, hub: h}
	if h.subs[projectID] == nil {
		h.subs[projectID] = map[*Sub]struct{}{}
	}
	h.subs[projectID][s] = struct{}{}
	h.total++
	if h.idle != nil {
		h.idle.Stop()
		h.idle = nil
	}
	if h.cancel == nil {
		h.start()
	}
	return s, nil
}

func (h *Hub) remove(s *Sub) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set := h.subs[s.project]
	if _, ok := set[s]; !ok {
		return // removed by Close of the hub
	}
	delete(set, s)
	if len(set) == 0 {
		delete(h.subs, s.project)
	}
	h.total--
	close(s.ch)
	if h.total == 0 && h.cancel != nil && !h.closed {
		gen := h.gen
		h.idle = time.AfterFunc(h.Linger, func() { h.stopIfIdle(gen) })
	}
}

func (h *Hub) stopIfIdle(gen uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.total == 0 && h.gen == gen && h.cancel != nil {
		h.cancel()
		h.cancel = nil
		h.idle = nil
	}
}

// start runs a consumer of a new generation; the caller holds mu. Records still in
// flight in a stopping consumer of an older generation are ignored, so a quick
// stop-start never delivers an event twice.
func (h *Hub) start() {
	h.gen++
	gen := h.gen
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		deliver := func(projectID uint32, value []byte) { h.deliver(gen, projectID, value) }
		for ctx.Err() == nil {
			if err := h.source.Run(ctx, deliver); err != nil && ctx.Err() == nil {
				h.logger.Warn("live consumer failed, restarting", "err", err)
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
		}
	}()
}

func (h *Hub) deliver(gen uint64, projectID uint32, value []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	set := h.subs[projectID]
	if len(set) == 0 || gen != h.gen {
		return // the common case: nobody watches this project, the JSON is not parsed
	}
	var rec event.Record
	if err := json.Unmarshal(value, &rec); err != nil {
		return
	}
	ev := view(rec)
	for s := range set {
		select {
		case s.ch <- ev:
		default:
			s.dropped.Add(1)
		}
	}
}

// Close stops the consumer and ends every stream, so that open SSE responses return
// and the server can shut down.
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	if h.cancel != nil {
		h.cancel()
		h.cancel = nil
	}
	if h.idle != nil {
		h.idle.Stop()
	}
	for _, set := range h.subs {
		for s := range set {
			close(s.ch)
		}
	}
	h.subs, h.total = map[uint32]map[*Sub]struct{}{}, 0
	h.mu.Unlock()
	h.wg.Wait()
}
