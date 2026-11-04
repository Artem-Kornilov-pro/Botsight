package httpapi_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/httpapi"
)

func TestMetrics(t *testing.T) {
	h := newHarnessDeps(t, func(d *httpapi.Deps) {
		d.Metrics = httpapi.NewMetrics(prometheus.NewRegistry(), func() int { return 3 })
	})
	c, _ := h.newUser()
	pid := c.createProject("metrics").num("project", "id")
	c.expect(http.StatusOK, "GET", path("/v1/projects/%d/errors", pid), nil)
	c.expect(http.StatusOK, "GET", path("/v1/projects/%d/errors", pid), nil) // from the cache
	c.do("GET", "/v1/nope", nil)

	resp, err := http.Get(h.srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	body := string(raw)
	for _, want := range []string{
		// route patterns, not paths with ids: bounded cardinality
		`botsight_api_requests_total{code="200",method="GET",route="/v1/projects/{projectID}/errors"} 2`,
		// an unknown path is counted under the router's pattern, not as itself
		`botsight_api_requests_total{code="404",method="GET",route="/v1/*"} 1`,
		`botsight_query_cache_total{kind="errors",result="hit"} 1`,
		`botsight_query_cache_total{kind="errors",result="miss"} 1`,
		`botsight_query_duration_seconds_count{kind="errors"} 1`,
		`botsight_live_streams 3`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(body, "project_id") || strings.Contains(body, "/v1/nope") {
		t.Error("no project ids or raw paths in labels")
	}
}
