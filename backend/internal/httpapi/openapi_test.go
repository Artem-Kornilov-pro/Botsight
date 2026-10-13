package httpapi

import (
	"bufio"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
)

// TestOpenAPICoversRoutes keeps backend/api/openapi.yaml in sync with the router:
// every route is documented and every documented operation exists.
func TestOpenAPICoversRoutes(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	spec, err := os.Open(filepath.Join(filepath.Dir(file), "..", "..", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer spec.Close()

	pathRe := regexp.MustCompile(`^  (/\S*):\s*$`)
	methodRe := regexp.MustCompile(`^    (get|post|put|patch|delete):`)
	documented := map[string]bool{}
	current := ""
	sc := bufio.NewScanner(spec)
	for sc.Scan() {
		line := sc.Text()
		if m := pathRe.FindStringSubmatch(line); m != nil {
			current = m[1]
		} else if m := methodRe.FindStringSubmatch(line); m != nil && current != "" {
			documented[strings.ToUpper(m[1])+" "+current] = true
		} else if line != "" && !strings.HasPrefix(line, " ") {
			current = "" // left the paths section
		}
	}

	routes := map[string]bool{}
	srv := New(Deps{Options: Options{Limits: DefaultLimits()}})
	err = chi.Walk(srv.Handler().(chi.Routes), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		if route != "/" {
			route = strings.TrimSuffix(route, "/")
		}
		routes[method+" "+route] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var missing, extra []string
	for r := range routes {
		if !documented[r] {
			missing = append(missing, r)
		}
	}
	for d := range documented {
		if !routes[d] {
			extra = append(extra, d)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	if len(missing) > 0 {
		t.Errorf("routes missing from openapi.yaml:\n  %s", strings.Join(missing, "\n  "))
	}
	if len(extra) > 0 {
		t.Errorf("openapi.yaml documents routes that do not exist:\n  %s", strings.Join(extra, "\n  "))
	}
	if len(routes) < 30 {
		t.Fatalf("walked only %d routes", len(routes))
	}
}
