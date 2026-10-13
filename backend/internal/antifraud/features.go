// Package antifraud estimates which users of each traffic source look like a bot farm
// (Botsight.md, 15).
//
// Per-user behaviour is computed from the event stream in memory: the Kafka key
// project_id:user_id gives every instance its own subset of users. Everything that
// needs users of all instances at once (sources, synchronous timing, spikes) and the
// score itself is computed by one ClickHouse query over all projects (analysis.go).
package antifraud

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
)

const (
	// FastInterval: a person reads the bot's reply and types or taps for longer;
	// "nobody types a full name in 200 ms" (Botsight.md, 15.1).
	FastInterval   = 400 * time.Millisecond
	signatureLen   = 5
	sampleLen      = 16
	signatureStepM = 100 // ms per bucket of a timing signature
)

type userKey struct {
	project uint32
	user    uint64
}

// userState is what an instance keeps per user, ~100 bytes.
type userState struct {
	first, last int64 // unix ms of the first and the last message in the window
	updates     uint32
	intervals   uint32
	fast        uint32
	sample      [sampleLen]uint32 // first intervals, for the median
	nSample     uint8
	sig         [signatureLen]uint8 // first intervals in 100 ms buckets
	nSig        uint8
	hasUsername bool
	isPremium   bool
	lang        string
	dirty       bool
}

// tracked reports whether an event is a message or a button press of a user:
// the actions whose timing a script gives away.
func tracked(r *event.Record) bool {
	return r.Type == "update" && r.UserID > 0 && (r.Name == "message" || r.Name == "callback_query")
}

func (s *userState) observe(r *event.Record) {
	if s.updates == 0 {
		s.first = r.TS
	} else if dt := r.TS - s.last; dt >= 0 { // out-of-order events give no interval
		s.intervals++
		if dt < FastInterval.Milliseconds() {
			s.fast++
		}
		if s.nSample < sampleLen {
			s.sample[s.nSample] = uint32(min(dt, 1<<31))
			s.nSample++
		}
		if s.nSig < signatureLen {
			s.sig[s.nSig] = uint8(min(dt/signatureStepM, 255))
			s.nSig++
		}
	}
	s.last = max(s.last, r.TS)
	s.updates++
	s.hasUsername, s.isPremium, s.lang = r.HasUsername, r.IsPremium, r.UserLang
	s.dirty = true
}

func (s *userState) median() uint32 {
	if s.nSample == 0 {
		return 0
	}
	v := slices.Clone(s.sample[:s.nSample])
	slices.Sort(v)
	return v[len(v)/2]
}

// signature is the rhythm of the first messages, "1.1.1.1.1": a script with fixed
// pauses gives the same signature to every account it drives.
func (s *userState) signature() string {
	if s.nSig < signatureLen {
		return ""
	}
	parts := make([]string, signatureLen)
	for i, b := range s.sig {
		parts[i] = strconv.Itoa(int(b))
	}
	return strings.Join(parts, ".")
}

// Features is a row of user_features.
type Features struct {
	ProjectID        uint32
	UserID           uint64
	FirstEvent       time.Time
	LastSeen         time.Time
	Updates          uint32
	Intervals        uint32
	FastIntervals    uint32
	MedianIntervalMS uint32
	Signature        string
	HasUsername      bool
	IsPremium        bool
	Lang             string
}

func (s *userState) features(k userKey) Features {
	return Features{
		ProjectID: k.project, UserID: k.user,
		FirstEvent: time.UnixMilli(s.first).UTC(), LastSeen: time.UnixMilli(s.last).UTC(),
		Updates: s.updates, Intervals: s.intervals, FastIntervals: s.fast,
		MedianIntervalMS: s.median(), Signature: s.signature(),
		HasUsername: s.hasUsername, IsPremium: s.isPremium, Lang: s.lang,
	}
}
