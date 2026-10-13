// Package event is the event contract between the SDK, the collector and ch-writer
// (Botsight.md, 9): parsing and validating SDK batches and the records that travel
// through Kafka to ClickHouse.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

// Limits of the collector (Botsight.md, 10.1).
const (
	SchemaVersion   = 1
	MaxBatchEvents  = 500
	MaxEventBytes   = 16 << 10
	MaxEventAge     = 7 * 24 * time.Hour
	MaxClockSkew    = 24 * time.Hour
	maxFutureSkew   = time.Hour
	maxTextRunes    = 4096
	maxProps        = 32
	maxPropKeyRunes = 64
	maxPropStrRunes = 1024
)

var (
	ErrSchemaVersion = errors.New("unsupported schema_version")
	ErrBatchID       = errors.New("batch_id must be a UUID")
	ErrBatchSize     = fmt.Errorf("a batch has 1 to %d events", MaxBatchEvents)
)

// Batch is POST /v1/batch as sent by the SDK.
type Batch struct {
	SchemaVersion int               `json:"schema_version"`
	BatchID       string            `json:"batch_id"`
	SentAt        string            `json:"sent_at"`
	SDK           SDKInfo           `json:"sdk"`
	Dropped       int64             `json:"dropped"`
	Events        []json.RawMessage `json:"events"`
}

type SDKInfo struct {
	Name             string `json:"name"`
	Version          string `json:"version"`
	Framework        string `json:"framework"`
	FrameworkVersion string `json:"framework_version"`
}

// ParseBatch decodes and checks the envelope; events are checked one by one by Normalize.
func ParseBatch(body []byte) (*Batch, error) {
	var b Batch
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if b.SchemaVersion != SchemaVersion {
		return nil, ErrSchemaVersion
	}
	if !IsUUID(b.BatchID) {
		return nil, ErrBatchID
	}
	if len(b.Events) == 0 || len(b.Events) > MaxBatchEvents {
		return nil, ErrBatchSize
	}
	if b.Dropped < 0 {
		b.Dropped = 0
	}
	return &b, nil
}

// wireEvent is one event as sent by the SDK (Botsight.md, 9.2-9.3).
type wireEvent struct {
	EventID      string                     `json:"event_id"`
	Type         string                     `json:"type"`
	Name         string                     `json:"name"`
	TS           string                     `json:"ts"`
	BotID        int64                      `json:"bot_id"`
	UserID       int64                      `json:"user_id"`
	ChatID       int64                      `json:"chat_id"`
	ChatType     string                     `json:"chat_type"`
	ContentType  string                     `json:"content_type"`
	Command      string                     `json:"command"`
	StartParam   string                     `json:"start_param"`
	CallbackData string                     `json:"callback_data"`
	State        string                     `json:"state"`
	Handler      string                     `json:"handler"`
	StateFrom    string                     `json:"state_from"`
	StateTo      string                     `json:"state_to"`
	MemberStatus string                     `json:"member_status"`
	Handled      bool                       `json:"handled"`
	OK           bool                       `json:"ok"`
	DurationMS   float64                    `json:"duration_ms"`
	ErrorCode    float64                    `json:"error_code"`
	Error        *string                    `json:"error"`
	TextLen      float64                    `json:"text_len"`
	Text         *string                    `json:"text"`
	User         *wireUser                  `json:"user"`
	Props        map[string]json.RawMessage `json:"props"`
}

type wireUser struct {
	Lang        string `json:"lang"`
	IsPremium   bool   `json:"is_premium"`
	HasUsername bool   `json:"has_username"`
}

var eventTypes = map[string]bool{"update": true, "step": true, "outgoing": true, "custom": true, "lifecycle": true}

// Record is a normalized event: the value of events.raw and a row of the events table.
type Record struct {
	ProjectID    uint32             `json:"project_id"`
	BotID        uint64             `json:"bot_id"`
	EventID      string             `json:"event_id"`
	TS           int64              `json:"ts"`          // unix ms
	ReceivedAt   int64              `json:"received_at"` // unix ms
	ClockSkewed  bool               `json:"clock_skewed,omitempty"`
	Type         string             `json:"type"`
	Name         string             `json:"name"`
	UserID       uint64             `json:"user_id,omitempty"`
	ChatID       int64              `json:"chat_id,omitempty"`
	ChatType     string             `json:"chat_type,omitempty"`
	ContentType  string             `json:"content_type,omitempty"`
	Command      string             `json:"command,omitempty"`
	StartParam   string             `json:"start_param,omitempty"`
	CallbackData string             `json:"callback_data,omitempty"`
	State        string             `json:"state,omitempty"`
	Handler      string             `json:"handler,omitempty"`
	StateFrom    string             `json:"state_from,omitempty"`
	StateTo      string             `json:"state_to,omitempty"`
	MemberStatus string             `json:"member_status,omitempty"`
	Handled      bool               `json:"handled,omitempty"`
	OK           bool               `json:"ok,omitempty"`
	DurationMS   uint32             `json:"duration_ms,omitempty"`
	ErrorCode    uint16             `json:"error_code,omitempty"`
	Error        string             `json:"error,omitempty"`
	TextLen      uint16             `json:"text_len,omitempty"`
	UserLang     string             `json:"user_lang,omitempty"`
	IsPremium    bool               `json:"is_premium,omitempty"`
	HasUsername  bool               `json:"has_username,omitempty"`
	PropsStr     map[string]string  `json:"props_str,omitempty"`
	PropsNum     map[string]float64 `json:"props_num,omitempty"`
}

// Text is a message text, split from the event into messages.text (Botsight.md, 9.4).
type Text struct {
	ProjectID uint32 `json:"project_id"`
	BotID     uint64 `json:"bot_id"`
	EventID   string `json:"event_id"`
	TS        int64  `json:"ts"`
	UserID    uint64 `json:"user_id,omitempty"`
	State     string `json:"state,omitempty"`
	Handled   bool   `json:"handled,omitempty"`
	Text      string `json:"text"`
}

// BatchMeta is one accepted batch, the value of ingest.batches.
type BatchMeta struct {
	ProjectID        uint32 `json:"project_id"`
	BatchID          string `json:"batch_id"`
	ReceivedAt       int64  `json:"received_at"`
	SDKName          string `json:"sdk_name"`
	SDKVersion       string `json:"sdk_version"`
	Framework        string `json:"framework"`
	FrameworkVersion string `json:"framework_version"`
	Events           uint32 `json:"events"`
	Rejected         uint32 `json:"rejected"`
	Dropped          uint32 `json:"dropped"`
}

// Alert is the value of the alerts topic, produced by ai-worker: topic trends and
// anomalies (Botsight.md, 14.1, 14.3). Details is a JSON document with the facts.
type Alert struct {
	ProjectID uint32 `json:"project_id"`
	AlertID   string `json:"alert_id"`
	Kind      string `json:"kind"`
	CreatedAt int64  `json:"created_at"` // unix ms
	Title     string `json:"title"`
	Text      string `json:"text"`
	Details   string `json:"details"`
}

// Normalizer turns SDK events into records for one batch.
type Normalizer struct {
	ProjectID  uint32
	ReceivedAt time.Time
	// ClockSkewed is set when the bot clock differs from the server by more than
	// MaxClockSkew (judged by sent_at): then every event gets received_at as ts.
	ClockSkewed bool
}

// NewNormalizer compares the batch sent_at with the server time (Botsight.md, 10.2).
func NewNormalizer(projectID uint32, b *Batch, receivedAt time.Time) *Normalizer {
	n := &Normalizer{ProjectID: projectID, ReceivedAt: receivedAt}
	if sent, err := time.Parse(time.RFC3339Nano, b.SentAt); err == nil {
		skew := receivedAt.Sub(sent)
		n.ClockSkewed = skew > MaxClockSkew || skew < -MaxClockSkew
	}
	return n
}

// Normalize validates one event. A non-nil error means the event is rejected; the
// rest of the batch is still accepted. text is non-nil only if the event carries one.
func (n *Normalizer) Normalize(raw json.RawMessage) (rec Record, text *Text, err error) {
	if len(raw) > MaxEventBytes {
		return rec, nil, errors.New("event is larger than 16 KiB")
	}
	var w wireEvent
	if err := json.Unmarshal(raw, &w); err != nil {
		return rec, nil, fmt.Errorf("invalid event JSON: %w", err)
	}
	if !IsUUID(w.EventID) {
		return rec, nil, errors.New("event_id must be a UUID")
	}
	if !eventTypes[w.Type] {
		return rec, nil, fmt.Errorf("unknown type %q", w.Type)
	}
	if w.Name == "" {
		return rec, nil, errors.New("name is required")
	}
	if w.BotID <= 0 || w.UserID < 0 {
		return rec, nil, errors.New("bot_id must be positive and user_id non-negative")
	}
	ts, err := time.Parse(time.RFC3339Nano, w.TS)
	if err != nil {
		return rec, nil, errors.New("ts must be RFC 3339")
	}

	rec = Record{
		ProjectID:    n.ProjectID,
		BotID:        uint64(w.BotID),
		EventID:      w.EventID,
		ReceivedAt:   n.ReceivedAt.UnixMilli(),
		Type:         w.Type,
		Name:         truncate(w.Name, 64),
		UserID:       uint64(w.UserID),
		ChatID:       w.ChatID,
		ChatType:     truncate(w.ChatType, 32),
		ContentType:  truncate(w.ContentType, 32),
		Command:      truncate(w.Command, 64),
		StartParam:   truncate(w.StartParam, 64),
		CallbackData: truncate(w.CallbackData, 64),
		State:        truncate(w.State, 256),
		Handler:      truncate(w.Handler, 256),
		StateFrom:    truncate(w.StateFrom, 256),
		StateTo:      truncate(w.StateTo, 256),
		MemberStatus: truncate(w.MemberStatus, 32),
		Handled:      w.Handled,
		OK:           w.OK,
		DurationMS:   uint32(clamp(w.DurationMS, math.MaxUint32)),
		ErrorCode:    uint16(clamp(w.ErrorCode, math.MaxUint16)),
		TextLen:      uint16(clamp(w.TextLen, math.MaxUint16)),
	}
	if w.Error != nil {
		rec.Error = truncate(*w.Error, 64)
	}
	if w.User != nil {
		rec.UserLang = truncate(w.User.Lang, 16)
		rec.IsPremium = w.User.IsPremium
		rec.HasUsername = w.User.HasUsername
	}

	switch age := n.ReceivedAt.Sub(ts); {
	case n.ClockSkewed || age < -maxFutureSkew:
		rec.TS = rec.ReceivedAt
		rec.ClockSkewed = true
	case age > MaxEventAge:
		return Record{}, nil, errors.New("event is older than 7 days")
	default:
		rec.TS = ts.UnixMilli()
	}

	n.props(&rec, w.Props)

	if w.Type == "update" && w.Text != nil && *w.Text != "" {
		text = &Text{
			ProjectID: rec.ProjectID, BotID: rec.BotID, EventID: rec.EventID, TS: rec.TS,
			UserID: rec.UserID, State: rec.State, Handled: rec.Handled,
			Text: truncate(*w.Text, maxTextRunes),
		}
	}
	return rec, text, nil
}

// props splits properties into strings and numbers (Botsight.md, 8.7): numbers stay
// numbers so ClickHouse can sum them; booleans become "true"/"false".
func (n *Normalizer) props(rec *Record, props map[string]json.RawMessage) {
	count := 0
	for k, v := range props {
		if count >= maxProps {
			break
		}
		if k == "" || utf8.RuneCountInString(k) > maxPropKeyRunes {
			continue
		}
		var s string
		var f float64
		var b bool
		switch {
		case json.Unmarshal(v, &s) == nil:
			if rec.PropsStr == nil {
				rec.PropsStr = map[string]string{}
			}
			rec.PropsStr[k] = truncate(s, maxPropStrRunes)
		case json.Unmarshal(v, &f) == nil && !math.IsInf(f, 0) && !math.IsNaN(f):
			if rec.PropsNum == nil {
				rec.PropsNum = map[string]float64{}
			}
			rec.PropsNum[k] = f
		case json.Unmarshal(v, &b) == nil:
			if rec.PropsStr == nil {
				rec.PropsStr = map[string]string{}
			}
			rec.PropsStr[k] = strconv.FormatBool(b)
		default:
			continue
		}
		count++
	}
}

// Key is the Kafka key of an event: project and user keep one user's events ordered in
// one partition and spread big projects over all partitions (Botsight.md, 11.1).
// Events without a user (outgoing requests) use the chat, which equals the user in private chats.
func Key(projectID uint32, userID uint64, chatID int64) []byte {
	b := make([]byte, 0, 24)
	b = strconv.AppendUint(b, uint64(projectID), 10)
	b = append(b, ':')
	if userID == 0 && chatID != 0 {
		return strconv.AppendInt(b, chatID, 10)
	}
	return strconv.AppendUint(b, userID, 10)
}

// IsUUID checks the canonical 8-4-4-4-12 hex form.
func IsUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < 36; i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
				return false
			}
		}
	}
	return true
}

func truncate(s string, maxRunes int) string {
	if len(s) <= maxRunes {
		return s
	}
	if !utf8.ValidString(s) {
		s = string([]rune(s)) // replaces invalid bytes
	}
	n := 0
	for i := range s {
		if n == maxRunes {
			return s[:i]
		}
		n++
	}
	return s
}

func clamp(v, max float64) float64 {
	if v < 0 || math.IsNaN(v) {
		return 0
	}
	if v > max {
		return max
	}
	return v
}
