package httpapi

import (
	"encoding/json"
	"testing"
)

func TestParseRubles(t *testing.T) {
	ok := map[string]int64{"0": 0, "990": 99000, "15000.5": 1500050, "0.01": 1, "9999999999.99": 999999999999}
	for in, want := range ok {
		got, err := parseRubles(json.Number(in))
		if err != nil || got != want {
			t.Errorf("parseRubles(%s) = %d, %v; want %d", in, got, err, want)
		}
		if back := string(formatRubles(want)); in != "15000.5" && in != "0" && in != "990" && back != in {
			t.Errorf("formatRubles(%d) = %s", want, back)
		}
	}
	for _, in := range []string{"", "-1", "1.234", "1e3", "01", "10000000000", "1.", ".5"} {
		if _, err := parseRubles(json.Number(in)); err == nil {
			t.Errorf("parseRubles(%q) accepted", in)
		}
	}
	if formatRubles(1500050) != "15000.50" || formatRubles(5) != "0.05" {
		t.Error("formatRubles")
	}
}

func TestNormalizeEmail(t *testing.T) {
	if got, err := normalizeEmail("  Ivan@Example.RU "); err != nil || got != "ivan@example.ru" {
		t.Fatalf("%q %v", got, err)
	}
	for _, in := range []string{"", "ivan", "ivan@", "@example.ru", "ivan@localhost", "Ivan <ivan@example.ru>", "a b@example.ru"} {
		if _, err := normalizeEmail(in); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
}

func TestValidateFunnelDefaults(t *testing.T) {
	d := FunnelDefinition{Steps: []FunnelStep{{Kind: "command", Value: " /start "}, {Kind: "custom", Value: "purchase"}}}
	if err := validateFunnel(&d); err != nil {
		t.Fatal(err)
	}
	if d.Steps[0].Value != "start" || d.WindowSeconds != defaultFunnelWindow {
		t.Fatalf("%+v", d)
	}
}
