package chwriter

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

// Sink maps one Kafka topic to one ClickHouse table.
type Sink struct {
	Topic   string
	Table   string
	Columns []string
	// Row decodes a Kafka value into values in Columns order; an error sends the
	// record to the DLQ.
	Row func(value []byte) ([]any, error)
}

func (s Sink) insertSQL() string {
	return "INSERT INTO " + s.Table + " (" + strings.Join(s.Columns, ", ") + ")"
}

// Sinks are the topics ch-writer consumes.
func Sinks() []Sink {
	return []Sink{EventsSink(), TextsSink(), BatchesSink()}
}

var errInvalidRecord = errors.New("invalid record")

func u8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func ms(v int64) time.Time { return time.UnixMilli(v).UTC() }

func EventsSink() Sink {
	return Sink{
		Topic: kafka.TopicEvents,
		Table: "events",
		Columns: []string{
			"project_id", "bot_id", "event_id", "ts", "received_at", "clock_skewed", "type", "name",
			"user_id", "chat_id", "chat_type", "content_type", "command", "start_param", "callback_data",
			"state", "handler", "state_from", "state_to", "member_status", "handled", "ok", "duration_ms",
			"error_code", "error", "text_len", "user_lang", "is_premium", "has_username", "props_str", "props_num",
		},
		Row: func(value []byte) ([]any, error) {
			var r event.Record
			if err := json.Unmarshal(value, &r); err != nil {
				return nil, err
			}
			if r.ProjectID == 0 || !event.IsUUID(r.EventID) || r.Type == "" || r.TS == 0 {
				return nil, errInvalidRecord
			}
			if r.PropsStr == nil {
				r.PropsStr = map[string]string{}
			}
			if r.PropsNum == nil {
				r.PropsNum = map[string]float64{}
			}
			return []any{
				r.ProjectID, r.BotID, r.EventID, ms(r.TS), ms(r.ReceivedAt), u8(r.ClockSkewed), r.Type, r.Name,
				r.UserID, r.ChatID, r.ChatType, r.ContentType, r.Command, r.StartParam, r.CallbackData,
				r.State, r.Handler, r.StateFrom, r.StateTo, r.MemberStatus, u8(r.Handled), u8(r.OK), r.DurationMS,
				r.ErrorCode, r.Error, r.TextLen, r.UserLang, u8(r.IsPremium), u8(r.HasUsername), r.PropsStr, r.PropsNum,
			}, nil
		},
	}
}

func TextsSink() Sink {
	return Sink{
		Topic:   kafka.TopicTexts,
		Table:   "messages_text",
		Columns: []string{"project_id", "bot_id", "event_id", "ts", "user_id", "state", "handled", "text"},
		Row: func(value []byte) ([]any, error) {
			var t event.Text
			if err := json.Unmarshal(value, &t); err != nil {
				return nil, err
			}
			if t.ProjectID == 0 || !event.IsUUID(t.EventID) || t.TS == 0 {
				return nil, errInvalidRecord
			}
			return []any{t.ProjectID, t.BotID, t.EventID, ms(t.TS), t.UserID, t.State, u8(t.Handled), t.Text}, nil
		},
	}
}

func BatchesSink() Sink {
	return Sink{
		Topic: kafka.TopicBatches,
		Table: "ingest_batches",
		Columns: []string{
			"project_id", "batch_id", "received_at", "sdk_name", "sdk_version", "framework",
			"framework_version", "events", "rejected", "dropped",
		},
		Row: func(value []byte) ([]any, error) {
			var b event.BatchMeta
			if err := json.Unmarshal(value, &b); err != nil {
				return nil, err
			}
			if b.ProjectID == 0 || !event.IsUUID(b.BatchID) || b.ReceivedAt == 0 {
				return nil, errInvalidRecord
			}
			return []any{
				b.ProjectID, b.BatchID, ms(b.ReceivedAt), b.SDKName, b.SDKVersion, b.Framework,
				b.FrameworkVersion, b.Events, b.Rejected, b.Dropped,
			}, nil
		},
	}
}
