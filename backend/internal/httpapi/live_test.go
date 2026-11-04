package httpapi_test

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/httpapi"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/live"
)

// topic is events.raw in memory for the live hub.
type topic chan [2]any

func (t topic) Run(ctx context.Context, deliver func(uint32, []byte)) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case r := <-t:
			deliver(r[0].(uint32), r[1].([]byte))
		}
	}
}

type sse struct {
	resp *http.Response
	rd   *bufio.Reader
}

func (c *client) stream(path string) *sse {
	c.h.t.Helper()
	req, _ := http.NewRequest("GET", c.h.srv.URL+path, nil)
	resp, err := c.http.Do(req)
	if err != nil {
		c.h.t.Fatal(err)
	}
	c.h.t.Cleanup(func() { resp.Body.Close() })
	return &sse{resp: resp, rd: bufio.NewReader(resp.Body)}
}

// next returns the next event's name, id and data, skipping comments.
func (s *sse) next(t *testing.T) (name, id, data string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			line, err := s.rd.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimRight(line, "\n")
			switch {
			case line == "" && name != "":
				return
			case strings.HasPrefix(line, "event: "):
				name = line[7:]
			case strings.HasPrefix(line, "id: "):
				id = line[4:]
			case strings.HasPrefix(line, "data: "):
				data = line[6:]
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("no SSE event")
	}
	return
}

func TestLiveStream(t *testing.T) {
	src := make(topic, 16)
	hub := live.NewHub(src, slog.New(slog.DiscardHandler))
	hub.MaxPerProject = 1
	h := newHarnessDeps(t, func(d *httpapi.Deps) { d.Live = hub })
	t.Cleanup(hub.Close)
	c, _ := h.newUser()
	pid := c.createProject("live").num("project", "id")
	p := path("/v1/projects/%d/live", pid)

	s := c.stream(p)
	if s.resp.StatusCode != 200 || s.resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("%d %v", s.resp.StatusCode, s.resp.Header)
	}
	if name, _, data := s.next(t); name != "ready" || !strings.Contains(data, `"project_id":`+strconv.Itoa(int(pid))) {
		t.Fatalf("%s %s", name, data)
	}

	push := func(project int64, rec event.Record) {
		b, _ := json.Marshal(rec)
		src <- [2]any{uint32(project), b}
	}
	push(pid+1000, event.Record{EventID: "other", Type: "update", Name: "message"})
	push(pid, event.Record{EventID: "0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d81", Type: "update", Name: "message",
		UserID: 9_100_000_000_000_000_001, Command: "start", StartParam: "vk", TS: 1_730_000_000_000})
	name, id, data := s.next(t)
	var ev map[string]any
	_ = json.Unmarshal([]byte(data), &ev)
	if name != "event" || id != "0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d81" || ev["user_id"] != "9100000000000000001" || ev["start_param"] != "vk" {
		t.Fatalf("%s %s %s", name, id, data)
	}

	// one stream per project here: the second tab is refused until the first closes
	if r := c.do("GET", p, nil); r.status != http.StatusTooManyRequests || r.str("error", "code") != "too_many_streams" {
		t.Fatalf("limit: %d %v", r.status, r.body)
	}
	stranger, _ := h.newUser()
	stranger.expect(http.StatusNotFound, "GET", p, nil)

	// shutdown ends the stream so that the server can stop
	hub.Close()
	if _, err := s.rd.ReadString('\n'); err == nil {
		if rest, _ := s.rd.ReadString('\n'); rest != "" {
			t.Fatalf("stream continues after Close: %q", rest)
		}
	}
}

func TestLiveWithoutKafka(t *testing.T) {
	h := newHarness(t)
	c, _ := h.newUser()
	p := path("/v1/projects/%d/live", c.createProject("x").num("project", "id"))
	if r := c.expect(http.StatusServiceUnavailable, "GET", p, nil); r.str("error", "code") != "live_unavailable" {
		t.Fatal(r.body)
	}
}
