package httpapi

import (
	"encoding/json"
	"net/mail"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

const (
	minPasswordLen = 8
	maxPasswordLen = 128
	maxNameLen     = 100
)

func normalizeEmail(raw string) (string, *fieldError) {
	email := strings.ToLower(strings.TrimSpace(raw))
	if email == "" {
		return "", invalid("email", "email is required")
	}
	addr, err := mail.ParseAddress(email)
	at := strings.LastIndexByte(email, '@')
	if err != nil || addr.Address != email || addr.Name != "" || len(email) > 254 ||
		at < 1 || !strings.Contains(email[at+1:], ".") {
		return "", invalid("email", "invalid email address")
	}
	return email, nil
}

func validatePassword(field, password string) *fieldError {
	n := utf8.RuneCountInString(password)
	if n < minPasswordLen {
		return invalid(field, "password must be at least 8 characters")
	}
	if n > maxPasswordLen {
		return invalid(field, "password must be at most 128 characters")
	}
	return nil
}

// cleanName trims a display name; empty is allowed only when required is false.
func cleanName(field, raw string, required bool) (string, *fieldError) {
	name := strings.TrimSpace(raw)
	if required && name == "" {
		return "", invalid(field, field+" is required")
	}
	if utf8.RuneCountInString(name) > maxNameLen {
		return "", invalid(field, field+" must be at most 100 characters")
	}
	if strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return "", invalid(field, field+" contains control characters")
	}
	return name, nil
}

func validRole(role string) bool {
	return role == store.RoleOwner || role == store.RoleEditor || role == store.RoleViewer
}

func validateSettings(s *store.ProjectSettings) *fieldError {
	if s.NeutralHandlers == nil {
		s.NeutralHandlers = []string{}
	}
	if len(s.NeutralHandlers) > 100 {
		return invalid("settings.neutral_handlers", "at most 100 handlers")
	}
	seen := map[string]bool{}
	out := s.NeutralHandlers[:0]
	for _, h := range s.NeutralHandlers {
		h = strings.TrimSpace(h)
		if h == "" || len(h) > 200 || !strings.Contains(h, ":") {
			return invalid("settings.neutral_handlers", "handlers are 'module:qualname', up to 200 characters")
		}
		if !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	s.NeutralHandlers = out
	return nil
}

// FunnelDefinition is the stored shape of a saved funnel (Botsight.md, 16.1):
// steps of a command, a button, an FSM state or a manual event.
type FunnelDefinition struct {
	Steps         []FunnelStep `json:"steps"`
	WindowSeconds int          `json:"window_seconds"`
	Breakdown     string       `json:"breakdown"`
}

type FunnelStep struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

const (
	defaultFunnelWindow = 24 * 60 * 60
	maxFunnelWindow     = 30 * 24 * 60 * 60
)

var funnelStepKinds = map[string]bool{"command": true, "callback_data": true, "state": true, "custom": true}

func validateFunnel(d *FunnelDefinition) *fieldError {
	if len(d.Steps) < 2 || len(d.Steps) > 20 {
		return invalid("definition.steps", "a funnel has 2 to 20 steps")
	}
	for i := range d.Steps {
		st := &d.Steps[i]
		st.Value = strings.TrimSpace(st.Value)
		if st.Kind == "command" {
			st.Value = strings.TrimPrefix(st.Value, "/")
		}
		if !funnelStepKinds[st.Kind] {
			return invalid("definition.steps", "step kind must be command, callback_data, state or custom")
		}
		if st.Value == "" || utf8.RuneCountInString(st.Value) > 256 {
			return invalid("definition.steps", "step value must be 1 to 256 characters")
		}
	}
	if d.WindowSeconds == 0 {
		d.WindowSeconds = defaultFunnelWindow
	}
	if d.WindowSeconds < 60 || d.WindowSeconds > maxFunnelWindow {
		return invalid("definition.window_seconds", "window must be from 60 seconds to 30 days")
	}
	if d.Breakdown != "" && d.Breakdown != "source" {
		return invalid("definition.breakdown", "breakdown must be empty or 'source'")
	}
	return nil
}

const dateLayout = "2006-01-02"

func parseDate(field, raw string) (time.Time, *fieldError) {
	t, err := time.Parse(dateLayout, raw)
	if err != nil {
		return time.Time{}, invalid(field, field+" must be a date YYYY-MM-DD")
	}
	return t, nil
}

var rublesRe = regexp.MustCompile(`^(0|[1-9][0-9]{0,9})(\.[0-9]{1,2})?$`)

// parseRubles parses a non-negative amount with up to 2 decimals into kopeks,
// without going through float64.
func parseRubles(n json.Number) (int64, *fieldError) {
	s := string(n)
	if !rublesRe.MatchString(s) {
		return 0, invalid("amount_rub", "amount must be a non-negative number with up to 2 decimals, below 10^10")
	}
	whole, frac, _ := strings.Cut(s, ".")
	rub, _ := strconv.ParseInt(whole, 10, 64)
	frac = (frac + "00")[:2]
	kop, _ := strconv.ParseInt(frac, 10, 64)
	return rub*100 + kop, nil
}

func formatRubles(kopeks int64) json.Number {
	return json.Number(strconv.FormatInt(kopeks/100, 10) + "." + leftPad2(kopeks%100))
}

func leftPad2(v int64) string {
	if v < 10 {
		return "0" + strconv.FormatInt(v, 10)
	}
	return strconv.FormatInt(v, 10)
}

// validSource checks a deep link label: Telegram allows A-Z, a-z, 0-9, _ and -, up to 64.
var sourceRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
