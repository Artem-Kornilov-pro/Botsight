package httpapi_test

import (
	"net/http"
	"testing"
)

func funnelBody(name string) map[string]any {
	return map[string]any{
		"name": name,
		"definition": map[string]any{
			"steps": []map[string]string{
				{"kind": "command", "value": "/start"},
				{"kind": "state", "value": "Form:name"},
				{"kind": "custom", "value": "lab_generated"},
			},
		},
	}
}

func TestFunnels(t *testing.T) {
	h := newHarness(t)
	c, _ := h.newUser()
	p := path("/v1/projects/%d", c.createProject("воронки").num("project", "id"))

	r := c.expect(http.StatusCreated, "POST", p+"/funnels", funnelBody("Анкета"))
	id := r.num("id")
	steps, _ := r.get("definition", "steps").([]any)
	first, _ := steps[0].(map[string]any)
	if len(steps) != 3 || first["value"] != "start" || r.num("definition", "window_seconds") != 86400 {
		t.Fatalf("not normalized: %v", r.body)
	}

	bad := []map[string]any{
		{"name": "x", "definition": map[string]any{"steps": []map[string]string{{"kind": "state", "value": "A"}}}},
		{"name": "x", "definition": map[string]any{"steps": []map[string]string{{"kind": "sql", "value": "A"}, {"kind": "state", "value": "B"}}}},
		{"name": "x", "definition": map[string]any{"steps": []map[string]string{{"kind": "state", "value": ""}, {"kind": "state", "value": "B"}}}},
		{"name": "", "definition": funnelBody("x")["definition"]},
		{"name": "x", "definition": map[string]any{"steps": funnelBody("x")["definition"].(map[string]any)["steps"], "window_seconds": 10}},
		{"name": "x", "definition": map[string]any{"steps": funnelBody("x")["definition"].(map[string]any)["steps"], "breakdown": "lang"}},
	}
	for _, b := range bad {
		if r := c.expect(http.StatusBadRequest, "POST", p+"/funnels", b); r.str("error", "code") != "validation_failed" {
			t.Errorf("%v: %v", b, r.body)
		}
	}

	fp := path("%s/funnels/%d", p, id)
	upd := funnelBody("Анкета v2")
	upd["definition"].(map[string]any)["window_seconds"] = 3600
	upd["definition"].(map[string]any)["breakdown"] = "source"
	if r := c.expect(http.StatusOK, "PUT", fp, upd); r.str("name") != "Анкета v2" || r.str("definition", "breakdown") != "source" {
		t.Fatalf("%v", r.body)
	}
	if r := c.expect(http.StatusOK, "GET", fp, nil); r.num("definition", "window_seconds") != 3600 {
		t.Fatalf("%v", r.body)
	}
	if list := c.expect(http.StatusOK, "GET", p+"/funnels", nil).list("funnels"); len(list) != 1 {
		t.Fatalf("%v", list)
	}
	c.expect(http.StatusNoContent, "DELETE", fp, nil)
	c.expect(http.StatusNotFound, "GET", fp, nil)
	c.expect(http.StatusNotFound, "PUT", fp, upd)

	// a funnel id from another project is not reachable through this one
	other := path("/v1/projects/%d", c.createProject("другой").num("project", "id"))
	foreign := c.expect(http.StatusCreated, "POST", other+"/funnels", funnelBody("чужая")).num("id")
	c.expect(http.StatusNotFound, "GET", path("%s/funnels/%d", p, foreign), nil)
	c.expect(http.StatusNotFound, "DELETE", path("%s/funnels/%d", p, foreign), nil)
}

func TestAdSpend(t *testing.T) {
	h := newHarness(t)
	c, _ := h.newUser()
	p := path("/v1/projects/%d", c.createProject("реклама").num("project", "id"))

	spend := func(source, from, to string, amount any) map[string]any {
		return map[string]any{"source": source, "from": from, "to": to, "amount_rub": amount}
	}
	r := c.expect(http.StatusCreated, "POST", p+"/ad-spend", spend("ch_tech", "2026-10-01", "2026-10-07", 15000.5))
	if r.str("from") != "2026-10-01" || r.str("to") != "2026-10-07" || r.get("amount_rub") != 15000.5 {
		t.Fatalf("%v", r.body)
	}
	if r := c.expect(http.StatusConflict, "POST", p+"/ad-spend", spend("ch_tech", "2026-10-07", "2026-10-10", 1)); r.str("error", "code") != "period_overlap" {
		t.Fatalf("overlap accepted: %v", r.body)
	}
	c.expect(http.StatusCreated, "POST", p+"/ad-spend", spend("ch_tech", "2026-10-08", "2026-10-14", 0))      // adjacent period
	c.expect(http.StatusCreated, "POST", p+"/ad-spend", spend("ch_memes", "2026-10-01", "2026-10-31", "990")) // another source

	for _, b := range []map[string]any{
		spend("bad label!", "2026-10-01", "2026-10-02", 1),
		spend("ok", "2026-10-02", "2026-10-01", 1),
		spend("ok", "2026-13-01", "2026-10-01", 1),
		spend("ok", "2026-01-01", "2027-06-01", 1),
		spend("ok", "2026-10-01", "2026-10-02", 1.234),
		spend("ok", "2026-10-01", "2026-10-02", -5),
	} {
		c.expect(http.StatusBadRequest, "POST", p+"/ad-spend", b)
	}

	all := c.expect(http.StatusOK, "GET", p+"/ad-spend", nil).list("ad_spend")
	if len(all) != 3 {
		t.Fatalf("%v", all)
	}
	week2 := c.expect(http.StatusOK, "GET", p+"/ad-spend?from=2026-10-10&to=2026-10-12", nil).list("ad_spend")
	if len(week2) != 2 { // ch_tech 08-14 and ch_memes 01-31
		t.Fatalf("filter: %v", week2)
	}
	c.expect(http.StatusBadRequest, "GET", p+"/ad-spend?from=yesterday", nil)

	id := int64(all[0]["id"].(float64))
	c.expect(http.StatusNoContent, "DELETE", path("%s/ad-spend/%d", p, id), nil)
	c.expect(http.StatusNotFound, "DELETE", path("%s/ad-spend/%d", p, id), nil)
}
