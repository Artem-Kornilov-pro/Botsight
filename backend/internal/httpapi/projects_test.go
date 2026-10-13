package httpapi_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/auth"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

func TestProjectLifecycleAndKeys(t *testing.T) {
	h := newHarness(t)
	c, _ := h.newUser()
	ctx := context.Background()

	revoked := env.Redis.Subscribe(ctx, auth.KeyRevocationChannel)
	defer revoked.Close()
	if _, err := revoked.Receive(ctx); err != nil { // subscription confirmation
		t.Fatal(err)
	}

	created := c.createProject("Магазин")
	projectID := created.num("project", "id")
	plain := created.str("api_key", "key")
	prefix, err := auth.ParseAPIKeyPrefix(plain)
	if err != nil || prefix != created.str("api_key", "prefix") {
		t.Fatalf("bad first key %q: %v", plain, err)
	}
	if created.str("project", "role") != "owner" || created.str("project", "plan") != "free" {
		t.Fatalf("%v", created.body)
	}

	// The collector lookup: prefix -> stored hash -> constant-time compare.
	stored, err := h.store.KeyByPrefix(ctx, prefix)
	if err != nil || !auth.VerifyAPIKey(plain, stored.SecretHash()) || int64(stored.ProjectID) != projectID || stored.RevokedAt != nil {
		t.Fatalf("KeyByPrefix: %+v %v", stored, err)
	}

	list := c.expect(http.StatusOK, "GET", "/v1/projects", nil).list("projects")
	if len(list) != 1 || int64(list[0]["id"].(float64)) != projectID {
		t.Fatalf("projects: %v", list)
	}

	p := path("/v1/projects/%d", projectID)
	r := c.expect(http.StatusOK, "PATCH", p, map[string]any{
		"name":     "Магазин 2",
		"settings": map[string]any{"capture_text": true, "neutral_handlers": []string{"bot.handlers:help", "bot.handlers:help"}},
	})
	if r.str("name") != "Магазин 2" || r.get("settings", "capture_text") != true ||
		len(r.get("settings", "neutral_handlers").([]any)) != 1 {
		t.Fatalf("update: %v", r.body)
	}
	c.expect(http.StatusBadRequest, "PATCH", p, map[string]any{"settings": map[string]any{"neutral_handlers": []string{"no-colon"}}})
	c.expect(http.StatusBadRequest, "PATCH", p, map[string]any{"name": " "})
	c.expect(http.StatusBadRequest, "PATCH", p, map[string]any{})
	if r := c.expect(http.StatusOK, "GET", p, nil); r.str("name") != "Магазин 2" {
		t.Fatalf("%v", r.body)
	}

	second := c.expect(http.StatusCreated, "POST", p+"/keys", map[string]string{"name": "staging"})
	if second.str("key") == "" || second.str("name") != "staging" {
		t.Fatalf("%v", second.body)
	}
	c.expect(http.StatusCreated, "POST", p+"/keys", nil) // body is optional

	keys := c.expect(http.StatusOK, "GET", p+"/keys", nil).list("keys")
	if len(keys) != 3 {
		t.Fatalf("keys: %v", keys)
	}
	for _, k := range keys {
		if _, ok := k["key"]; ok {
			t.Fatal("key list exposes a secret")
		}
	}

	keyPath := path("%s/keys/%d", p, second.num("id"))
	c.expect(http.StatusNoContent, "DELETE", keyPath, nil)
	expectRevoked(t, revoked.Channel(), second.str("prefix"))
	c.expect(http.StatusNoContent, "DELETE", keyPath, nil) // idempotent
	c.expect(http.StatusNotFound, "DELETE", p+"/keys/999999", nil)
	if k, _ := h.store.KeyByPrefix(ctx, second.str("prefix")); k.RevokedAt == nil {
		t.Fatal("key not revoked")
	}

	// Deleting the project revokes its remaining active keys for collectors.
	c.expect(http.StatusNoContent, "DELETE", p, nil)
	expectRevoked(t, revoked.Channel(), prefix)
	c.expect(http.StatusNotFound, "GET", p, nil)
	if _, err := h.store.KeyByPrefix(ctx, prefix); err != store.ErrNotFound {
		t.Fatalf("key survived project deletion: %v", err)
	}
}

func expectRevoked(t *testing.T, ch <-chan *redis.Message, prefix string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case msg := <-ch:
			if msg.Payload == prefix {
				return
			}
		case <-deadline:
			t.Fatalf("no revocation published for %s", prefix)
		}
	}
}

func TestProjectIsolation(t *testing.T) {
	h := newHarness(t)
	owner, _ := h.newUser()
	stranger, _ := h.newUser()
	p := path("/v1/projects/%d", owner.createProject("чужой").num("project", "id"))

	for _, req := range []struct{ method, path string }{
		{"GET", p}, {"PATCH", p}, {"DELETE", p},
		{"GET", p + "/members"}, {"GET", p + "/keys"}, {"POST", p + "/keys"},
		{"GET", p + "/funnels"}, {"GET", p + "/ad-spend"},
	} {
		var body any
		if req.method == "PATCH" {
			body = map[string]string{"name": "hacked"}
		}
		if r := stranger.do(req.method, req.path, body); r.status != http.StatusNotFound {
			t.Errorf("%s %s by a stranger: %d %v", req.method, req.path, r.status, r.body)
		}
	}
	if list := stranger.expect(http.StatusOK, "GET", "/v1/projects", nil).list("projects"); len(list) != 0 {
		t.Fatalf("stranger sees projects: %v", list)
	}
	stranger.expect(http.StatusNotFound, "GET", "/v1/projects/abc", nil)
	owner.expect(http.StatusOK, "GET", p, nil)
}

func TestMembersAndRoles(t *testing.T) {
	h := newHarness(t)
	owner, _ := h.newUser()
	member, memberEmail := h.newUser()
	created := owner.createProject("команда")
	ownerID := owner.expect(http.StatusOK, "GET", "/v1/account", nil).num("account", "id")
	memberID := member.expect(http.StatusOK, "GET", "/v1/account", nil).num("account", "id")
	p := path("/v1/projects/%d", created.num("project", "id"))

	r := owner.expect(http.StatusCreated, "POST", p+"/members", map[string]string{"email": memberEmail, "role": "viewer"})
	if r.num("account_id") != memberID || r.str("role") != "viewer" || r.str("email") != memberEmail {
		t.Fatalf("add member: %v", r.body)
	}
	if r := owner.expect(http.StatusConflict, "POST", p+"/members", map[string]string{"email": memberEmail, "role": "editor"}); r.str("error", "code") != "already_member" {
		t.Fatalf("%v", r.body)
	}
	if r := owner.expect(http.StatusNotFound, "POST", p+"/members", map[string]string{"email": "ghost-" + memberEmail, "role": "viewer"}); r.str("error", "code") != "account_not_found" {
		t.Fatalf("%v", r.body)
	}
	owner.expect(http.StatusBadRequest, "POST", p+"/members", map[string]string{"email": memberEmail, "role": "admin"})

	// viewer: read only
	if r := member.expect(http.StatusOK, "GET", p, nil); r.str("role") != "viewer" {
		t.Fatalf("%v", r.body)
	}
	member.expect(http.StatusOK, "GET", p+"/members", nil)
	member.expect(http.StatusOK, "GET", p+"/keys", nil)
	if r := member.expect(http.StatusForbidden, "PATCH", p, map[string]string{"name": "x"}); r.str("error", "code") != "forbidden" {
		t.Fatalf("%v", r.body)
	}
	member.expect(http.StatusForbidden, "POST", p+"/funnels", funnelBody("x"))

	// editor: settings and dashboard data, but not keys or members
	owner.expect(http.StatusOK, "PATCH", path("%s/members/%d", p, memberID), map[string]string{"role": "editor"})
	member.expect(http.StatusOK, "PATCH", p, map[string]string{"name": "переименовал редактор"})
	member.expect(http.StatusForbidden, "POST", p+"/keys", nil)
	member.expect(http.StatusForbidden, "POST", p+"/members", map[string]string{"email": memberEmail, "role": "owner"})
	member.expect(http.StatusForbidden, "DELETE", path("%s/members/%d", p, ownerID), nil)
	member.expect(http.StatusForbidden, "DELETE", p, nil)

	// the project never loses its last owner
	if r := owner.expect(http.StatusConflict, "PATCH", path("%s/members/%d", p, ownerID), map[string]string{"role": "viewer"}); r.str("error", "code") != "last_owner" {
		t.Fatalf("%v", r.body)
	}
	owner.expect(http.StatusConflict, "DELETE", path("%s/members/%d", p, ownerID), nil)
	owner.expect(http.StatusNotFound, "PATCH", p+"/members/999999999", map[string]string{"role": "viewer"})

	// with a second owner the first one may leave
	owner.expect(http.StatusOK, "PATCH", path("%s/members/%d", p, memberID), map[string]string{"role": "owner"})
	owner.expect(http.StatusNoContent, "DELETE", path("%s/members/%d", p, ownerID), nil)
	owner.expect(http.StatusNotFound, "GET", p, nil)
	members := member.expect(http.StatusOK, "GET", p+"/members", nil).list("members")
	if len(members) != 1 || members[0]["role"] != "owner" {
		t.Fatalf("members: %v", members)
	}
}

func TestMemberCanLeave(t *testing.T) {
	h := newHarness(t)
	owner, _ := h.newUser()
	viewer, viewerEmail := h.newUser()
	p := path("/v1/projects/%d", owner.createProject("p").num("project", "id"))
	owner.expect(http.StatusCreated, "POST", p+"/members", map[string]string{"email": viewerEmail, "role": "viewer"})
	viewerID := viewer.expect(http.StatusOK, "GET", "/v1/account", nil).num("account", "id")
	viewer.expect(http.StatusNoContent, "DELETE", path("%s/members/%d", p, viewerID), nil)
	viewer.expect(http.StatusNotFound, "GET", p, nil)
}
