package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/testenv"
)

var (
	env    *testenv.Env
	envErr error
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	env, envErr = testenv.StartWith(ctx, testenv.Options{Postgres: true})
	cancel()
	code := m.Run()
	if env != nil {
		env.Close()
	}
	os.Exit(code)
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	if envErr != nil {
		if os.Getenv("CI") != "" {
			t.Fatal(envErr)
		}
		t.Skipf("Docker: %v", envErr)
	}
	return store.New(env.Pool)
}

var seq int

func newProject(t *testing.T, s *store.Store) (int32, int64) {
	t.Helper()
	seq++
	ctx := context.Background()
	acc, err := s.CreateAccount(ctx, fmt.Sprintf("bc-%d-%d@example.com", time.Now().UnixNano(), seq), "x", "")
	if err != nil {
		t.Fatal(err)
	}
	p, _, err := s.CreateProject(ctx, "bot", acc.ID, store.NewKey{
		Prefix: fmt.Sprintf("bs_live_%08d", time.Now().UnixNano()%1e8+int64(seq)), Hash: []byte("h"), Name: "k",
	})
	if err != nil {
		t.Fatal(err)
	}
	return p.ID, acc.ID
}

func chats(n int, from int64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = from + int64(i)
	}
	return out
}

func TestBroadcastLeaseIsExclusive(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pid, acc := newProject(t, s)
	b, err := s.CreateBroadcast(ctx, pid, "Скидка 20%", "HTML", []byte(`{}`), chats(1000, 1), acc)
	if err != nil || b.Total != 1000 || b.Status != "sending" {
		t.Fatalf("%+v %v", b, err)
	}

	// ten replicas of one bot lease at once: every recipient is given exactly once
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				l, _, err := s.LeaseRecipients(ctx, pid, 37, time.Minute, 3)
				if err != nil {
					t.Error(err)
					return
				}
				if l == nil {
					return
				}
				if l.Text != "Скидка 20%" || l.ParseMode != "HTML" || l.Until.Before(time.Now()) {
					t.Errorf("lease %+v", l)
				}
				mu.Lock()
				for _, id := range l.ChatIDs {
					seen[id]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != 1000 {
		t.Fatalf("leased %d recipients", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("chat %d leased %d times", id, n)
		}
	}
}

func TestBroadcastLifecycle(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pid, acc := newProject(t, s)
	other, _ := newProject(t, s)
	b, err := s.CreateBroadcast(ctx, pid, "hi", "", []byte(`{"active_days":30}`), chats(5, 100), acc)
	if err != nil {
		t.Fatal(err)
	}

	l, active, err := s.LeaseRecipients(ctx, pid, 3, time.Minute, 3)
	if err != nil || !active || len(l.ChatIDs) != 3 {
		t.Fatalf("%+v %v %v", l, active, err)
	}
	// another project cannot report on this broadcast
	if n, err := s.ReportResults(ctx, other, b.ID, []store.DeliveryResult{{ChatID: 100, Status: "sent"}}); err != nil || n != 0 {
		t.Fatalf("foreign report: %d %v", n, err)
	}
	report := []store.DeliveryResult{
		{ChatID: 100, Status: "sent"}, {ChatID: 101, Status: "blocked", Error: "bot_blocked"},
		{ChatID: 102, Status: "failed", Error: "bad_request"},
	}
	if n, err := s.ReportResults(ctx, pid, b.ID, report); err != nil || n != 3 {
		t.Fatalf("report: %d %v", n, err)
	}
	if n, _ := s.ReportResults(ctx, pid, b.ID, report); n != 0 { // the SDK retried the report
		t.Fatalf("repeated report changed %d rows", n)
	}

	// the bot died holding a lease: the recipients come back after it expires
	l, _, _ = s.LeaseRecipients(ctx, pid, 10, 50*time.Millisecond, 3)
	if l == nil || len(l.ChatIDs) != 2 {
		t.Fatalf("%+v", l)
	}
	if l2, active, _ := s.LeaseRecipients(ctx, pid, 10, time.Minute, 3); l2 != nil || !active {
		t.Fatalf("leased twice: %+v", l2)
	}
	time.Sleep(100 * time.Millisecond)
	l, _, _ = s.LeaseRecipients(ctx, pid, 10, 50*time.Millisecond, 3)
	if l == nil || len(l.ChatIDs) != 2 {
		t.Fatalf("expired lease not returned: %+v", l)
	}
	// third expired lease: out of attempts, failed, the broadcast is done
	time.Sleep(100 * time.Millisecond)
	_, _, _ = s.LeaseRecipients(ctx, pid, 10, 50*time.Millisecond, 3)
	time.Sleep(100 * time.Millisecond)
	l, active, err = s.LeaseRecipients(ctx, pid, 10, time.Minute, 3)
	if err != nil || l != nil || active {
		t.Fatalf("after attempts: %+v %v %v", l, active, err)
	}
	got, err := s.Broadcast(ctx, pid, b.ID)
	if err != nil || got.Status != "done" || got.FinishedAt == nil || got.Stats.Sent != 1 || got.Stats.Blocked != 1 ||
		got.Stats.Failed != 3 || got.Stats.Pending != 0 || got.Stats.Errors["not_confirmed"] != 2 || got.Stats.Errors["bot_blocked"] != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := s.Broadcast(ctx, other, b.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("foreign read: %v", err)
	}
	if err := s.CancelBroadcast(ctx, pid, b.ID); !errors.Is(err, store.ErrNotSending) {
		t.Fatalf("cancel done: %v", err)
	}
}

func TestBroadcastCancelAndOrder(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pid, acc := newProject(t, s)
	first, _ := s.CreateBroadcast(ctx, pid, "first", "", []byte(`{}`), chats(2, 1), acc)
	second, _ := s.CreateBroadcast(ctx, pid, "second", "", []byte(`{}`), chats(2, 1), acc)

	if err := s.CancelBroadcast(ctx, pid, first.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.CancelBroadcast(ctx, pid+100000, first.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("foreign cancel: %v", err)
	}
	l, _, _ := s.LeaseRecipients(ctx, pid, 10, time.Minute, 3)
	if l == nil || l.BroadcastID != second.ID {
		t.Fatalf("canceled broadcast leased: %+v", l)
	}
	list, err := s.Broadcasts(ctx, pid)
	if err != nil || len(list) != 2 || list[0].ID != second.ID || list[1].Status != "canceled" || list[1].Stats.Pending != 2 {
		t.Fatalf("%+v %v", list, err)
	}
	ids := append([]int64(nil), l.ChatIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) != 2 || ids[0] != 1 {
		t.Fatal(ids)
	}
}
