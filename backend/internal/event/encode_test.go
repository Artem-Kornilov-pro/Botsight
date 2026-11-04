package event

import (
	"bytes"
	"encoding/json"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
)

var nasty = []string{
	"", "plain", `quote " and \ backslash`, "line\nbreak\ttab\rcr", "\b\f\x00\x01\x1f\x7f",
	"<script>&amp;</script>", "кириллица", "emoji 👍🏽", "  ", "bad \xff\xfe utf8", "\xc3",
	"mixed \"<\xff>\"   конец",
}

var floats = []float64{0, 1, -1, 0.5, 990, 1e-6, 9.99e-7, 1e20, 1e21, 123456789.123, -2.5e-9, math.MaxFloat64, math.SmallestNonzeroFloat64, 3.14159}

func randomRecord(r *rand.Rand) Record {
	s := func() string { return nasty[r.IntN(len(nasty))] }
	rec := Record{
		ProjectID: r.Uint32(), BotID: r.Uint64(), EventID: s(), TS: r.Int64() - math.MaxInt64/2, ReceivedAt: r.Int64(),
		ClockSkewed: r.IntN(2) == 0, Type: s(), Name: s(),
	}
	if r.IntN(2) == 0 {
		rec.UserID, rec.ChatID = r.Uint64(), -r.Int64()
	}
	for _, f := range []*string{&rec.ChatType, &rec.ContentType, &rec.Command, &rec.StartParam, &rec.CallbackData,
		&rec.State, &rec.Handler, &rec.StateFrom, &rec.StateTo, &rec.MemberStatus, &rec.Error, &rec.UserLang} {
		if r.IntN(3) == 0 {
			*f = s()
		}
	}
	rec.Handled, rec.OK, rec.IsPremium, rec.HasUsername = r.IntN(2) == 0, r.IntN(2) == 0, r.IntN(2) == 0, r.IntN(2) == 0
	if r.IntN(2) == 0 {
		rec.DurationMS, rec.ErrorCode, rec.TextLen = r.Uint32(), uint16(r.Uint32()), uint16(r.Uint32())
	}
	if r.IntN(3) == 0 {
		rec.PropsStr = map[string]string{}
		for range r.IntN(5) + 1 {
			rec.PropsStr[s()+string(rune('a'+r.IntN(26)))] = s()
		}
	}
	if r.IntN(3) == 0 {
		rec.PropsNum = map[string]float64{}
		for range r.IntN(5) + 1 {
			rec.PropsNum[s()+string(rune('a'+r.IntN(26)))] = floats[r.IntN(len(floats))] * float64(1-2*r.IntN(2))
		}
	}
	return rec
}

// The encoder must write byte for byte what json.Marshal writes: ch-writer, antifraud and
// /live decode these records, and a difference would only show in production.
func TestAppendJSONMatchesMarshal(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for i := range 20_000 {
		rec := randomRecord(r)
		want, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		got, err := rec.AppendJSON(nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("record %d differs\n got: %s\nwant: %s", i, got, want)
		}
	}
	// appends after what the buffer already holds
	rec := randomRecord(r)
	want, _ := json.Marshal(rec)
	got, _ := rec.AppendJSON([]byte("prefix"))
	if !bytes.Equal(got, append([]byte("prefix"), want...)) {
		t.Fatal("does not append")
	}
	if _, err := (&Record{PropsNum: map[string]float64{"x": math.NaN()}}).AppendJSON(nil); err == nil {
		t.Fatal("NaN has no JSON form, like json.Marshal")
	}
}

func TestAppendKey(t *testing.T) {
	for _, c := range []struct {
		p    uint32
		u    uint64
		chat int64
	}{{42, 7, 7}, {42, 0, -100}, {1, 0, 0}, {math.MaxUint32, math.MaxUint64, 5}} {
		if got, want := AppendKey(nil, c.p, c.u, c.chat), Key(c.p, c.u, c.chat); !bytes.Equal(got, want) {
			t.Fatalf("%v: %s, want %s", c, got, want)
		}
	}
}

// propsOracle is how props were parsed before the first-byte dispatch: json.Unmarshal as
// a string, then a number, then a bool. The fast path must give the same result.
func propsOracle(v json.RawMessage) (str string, num float64, kind string) {
	var b bool
	switch {
	case json.Unmarshal(v, &str) == nil:
		return truncate(str, maxPropStrRunes), 0, "str"
	case json.Unmarshal(v, &num) == nil && !math.IsInf(num, 0) && !math.IsNaN(num):
		return "", num, "num"
	case json.Unmarshal(v, &b) == nil:
		if b {
			return "true", 0, "str"
		}
		return "false", 0, "str"
	}
	return "", 0, "skip"
}

func TestPropsMatchUnmarshal(t *testing.T) {
	values := []string{
		`"plain"`, `""`, `"кириллица"`, `"esc \" \\ \n é 👍"`, "\"bad \xff utf8\"", "\"\xc3\"",
		`"` + strings.Repeat("я", 300) + `"`, `0`, `-0`, `990`, `-1.5e-7`, `1e308`, `1e400`, `-1e400`, `12345678901234567890`,
		`true`, `false`, `null`, `{"a":1}`, `[1,2]`, ` 42 `, "\t\"x\"\n",
	}
	n := &Normalizer{}
	for _, v := range values {
		var rec Record
		n.props(&rec, map[string]json.RawMessage{"k": json.RawMessage(v)})
		str, num, kind := propsOracle(json.RawMessage(v))
		gotStr, isStr := rec.PropsStr["k"]
		gotNum, isNum := rec.PropsNum["k"]
		switch kind {
		case "str":
			if !isStr || isNum || gotStr != str {
				t.Errorf("%s: got %q/%v, want string %q", v, gotStr, rec.PropsNum, str)
			}
		case "num":
			if !isNum || isStr || gotNum != num {
				t.Errorf("%s: got %v/%v, want number %v", v, rec.PropsStr, gotNum, num)
			}
		default:
			if isStr || isNum {
				t.Errorf("%s: want skipped, got %v %v", v, rec.PropsStr, rec.PropsNum)
			}
		}
	}
}
