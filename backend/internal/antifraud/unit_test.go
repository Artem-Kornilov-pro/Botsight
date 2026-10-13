package antifraud

import (
	"strings"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
)

func msg(ts int64) *event.Record {
	return &event.Record{Type: "update", Name: "message", UserID: 1, TS: ts, UserLang: "ru", HasUsername: true}
}

func TestUserStateBot(t *testing.T) {
	var s userState
	for i := int64(0); i < 8; i++ {
		s.observe(msg(1_000_000 + i*150)) // a script: a message every 150 ms
	}
	f := s.features(userKey{7, 1})
	if f.Updates != 8 || f.Intervals != 7 || f.FastIntervals != 7 || f.MedianIntervalMS != 150 {
		t.Fatalf("%+v", f)
	}
	if f.Signature != "1.1.1.1.1" {
		t.Fatalf("signature %q", f.Signature)
	}
	if !f.FirstEvent.Equal(time.UnixMilli(1_000_000)) || f.LastSeen.Sub(f.FirstEvent) != 1050*time.Millisecond {
		t.Fatalf("times %v %v", f.FirstEvent, f.LastSeen)
	}
}

func TestUserStateHuman(t *testing.T) {
	var s userState
	ts := int64(1_000_000)
	for _, gap := range []int64{0, 3200, 12_500, 1800, 45_000, 2600} {
		ts += gap
		s.observe(msg(ts))
	}
	f := s.features(userKey{7, 1})
	if f.FastIntervals != 0 || f.Signature != "32.125.18.255.26" {
		t.Fatalf("%+v", f)
	}
	if f.MedianIntervalMS != 3200 {
		t.Fatalf("median %d", f.MedianIntervalMS)
	}
}

func TestUserStateIgnoresOutOfOrderAndShortHistory(t *testing.T) {
	var s userState
	s.observe(msg(5000))
	s.observe(msg(4000)) // late event: no interval
	s.observe(msg(5100))
	f := s.features(userKey{1, 1})
	if f.Intervals != 1 || f.Signature != "" || f.LastSeen != time.UnixMilli(5100).UTC() {
		t.Fatalf("%+v", f)
	}
}

func TestTracked(t *testing.T) {
	cases := []struct {
		r    event.Record
		want bool
	}{
		{event.Record{Type: "update", Name: "message", UserID: 1}, true},
		{event.Record{Type: "update", Name: "callback_query", UserID: 1}, true},
		{event.Record{Type: "update", Name: "my_chat_member", UserID: 1}, false},
		{event.Record{Type: "step", Name: "message", UserID: 1}, false},
		{event.Record{Type: "update", Name: "message"}, false},
	}
	for _, c := range cases {
		if tracked(&c.r) != c.want {
			t.Errorf("%+v", c.r)
		}
	}
}

func TestUnionFind(t *testing.T) {
	uf := newUnionFind()
	uf.union(1, 2)
	uf.union(3, 4)
	uf.union(2, 4)
	if uf.find(1) != uf.find(3) || uf.size[uf.find(1)] != 4 {
		t.Fatal("components not merged")
	}
	if uf.find(5) == uf.find(1) {
		t.Fatal("unrelated node merged")
	}
}

func TestRings(t *testing.T) {
	members := map[uint64]member{}
	var edges []edge
	// a chain of 12 suspicious accounts inviting each other
	for i := uint64(100); i < 112; i++ {
		edges = append(edges, edge{invitee: i + 1, inviter: i})
		members[i] = member{suspicious: true, known: true}
	}
	// a star: one inviter, 35 accounts without usernames, behaving like people
	for i := uint64(200); i < 235; i++ {
		edges = append(edges, edge{invitee: i, inviter: 199})
		members[i] = member{known: true}
	}
	// a normal referral: 15 friends with usernames
	for i := uint64(300); i < 315; i++ {
		edges = append(edges, edge{invitee: i, inviter: 299})
		members[i] = member{known: true, hasUsername: true}
	}
	// a small suspicious pair is too small to be a ring
	edges = append(edges, edge{invitee: 401, inviter: 400})
	members[401], members[400] = member{suspicious: true, known: true}, member{suspicious: true, known: true}

	got := rings(edges, members)
	if got[105] != 13 || got[199] != 36 || got[234] != 36 {
		t.Fatalf("rings not flagged: %v", got)
	}
	if _, ok := got[300]; ok {
		t.Fatal("friends flagged")
	}
	if _, ok := got[401]; ok {
		t.Fatal("pair flagged")
	}
}

func TestSourceAlert(t *testing.T) {
	at := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	a := sourceAlert(42, "promo_farm", 412, 358, map[string]uint32{"synced_timing": 340, "fast_steps": 350, "start_spike": 10}, at)
	if a.Kind != "fraud" || a.ProjectID != 42 || !strings.Contains(a.Title, "«promo_farm»: 87%") {
		t.Fatalf("%+v", a)
	}
	if !strings.Contains(a.Text, "слишком быстрые ответы (98%)") || !strings.Contains(a.Text, "пользователей: 412, из них подозрительных: 358") {
		t.Fatalf("%s", a.Text)
	}
	again := sourceAlert(42, "promo_farm", 500, 450, nil, at.Add(time.Hour))
	if again.AlertID != a.AlertID || !event.IsUUID(a.AlertID) {
		t.Fatal("one alert per source a day")
	}
	if sourceAlert(42, "", 100, 60, nil, at).Title != "Источник «без метки»: 60% пользователей похожи на ботоферму" {
		t.Fatal("empty source label")
	}
}
