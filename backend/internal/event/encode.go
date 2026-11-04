package event

import (
	"errors"
	"math"
	"slices"
	"strconv"
	"unicode/utf8"
)

// AppendJSON appends the record as JSON, the same document json.Marshal produces: fields
// in struct order, omitempty as tagged, encoding/json's string escaping and float
// format. It is the collector's hot path (Botsight.md, 10.5): json.Marshal reflects over
// 31 fields and copies its result for every event; this writes into one buffer per
// batch. TestAppendJSONMatchesMarshal keeps the two identical.
func (r *Record) AppendJSON(b []byte) ([]byte, error) {
	b = append(b, `{"project_id":`...)
	b = strconv.AppendUint(b, uint64(r.ProjectID), 10)
	b = append(b, `,"bot_id":`...)
	b = strconv.AppendUint(b, r.BotID, 10)
	b = append(b, `,"event_id":`...)
	b = appendString(b, r.EventID)
	b = append(b, `,"ts":`...)
	b = strconv.AppendInt(b, r.TS, 10)
	b = append(b, `,"received_at":`...)
	b = strconv.AppendInt(b, r.ReceivedAt, 10)
	if r.ClockSkewed {
		b = append(b, `,"clock_skewed":true`...)
	}
	b = append(b, `,"type":`...)
	b = appendString(b, r.Type)
	b = append(b, `,"name":`...)
	b = appendString(b, r.Name)
	if r.UserID != 0 {
		b = append(b, `,"user_id":`...)
		b = strconv.AppendUint(b, r.UserID, 10)
	}
	if r.ChatID != 0 {
		b = append(b, `,"chat_id":`...)
		b = strconv.AppendInt(b, r.ChatID, 10)
	}
	b = appendOptional(b, `,"chat_type":`, r.ChatType)
	b = appendOptional(b, `,"content_type":`, r.ContentType)
	b = appendOptional(b, `,"command":`, r.Command)
	b = appendOptional(b, `,"start_param":`, r.StartParam)
	b = appendOptional(b, `,"callback_data":`, r.CallbackData)
	b = appendOptional(b, `,"state":`, r.State)
	b = appendOptional(b, `,"handler":`, r.Handler)
	b = appendOptional(b, `,"state_from":`, r.StateFrom)
	b = appendOptional(b, `,"state_to":`, r.StateTo)
	b = appendOptional(b, `,"member_status":`, r.MemberStatus)
	if r.Handled {
		b = append(b, `,"handled":true`...)
	}
	if r.OK {
		b = append(b, `,"ok":true`...)
	}
	if r.DurationMS != 0 {
		b = append(b, `,"duration_ms":`...)
		b = strconv.AppendUint(b, uint64(r.DurationMS), 10)
	}
	if r.ErrorCode != 0 {
		b = append(b, `,"error_code":`...)
		b = strconv.AppendUint(b, uint64(r.ErrorCode), 10)
	}
	b = appendOptional(b, `,"error":`, r.Error)
	if r.TextLen != 0 {
		b = append(b, `,"text_len":`...)
		b = strconv.AppendUint(b, uint64(r.TextLen), 10)
	}
	b = appendOptional(b, `,"user_lang":`, r.UserLang)
	if r.IsPremium {
		b = append(b, `,"is_premium":true`...)
	}
	if r.HasUsername {
		b = append(b, `,"has_username":true`...)
	}
	if len(r.PropsStr) > 0 {
		b = append(b, `,"props_str":{`...)
		for i, k := range slices.Sorted(mapKeys(r.PropsStr)) { // json.Marshal sorts map keys
			if i > 0 {
				b = append(b, ',')
			}
			b = appendString(b, k)
			b = append(b, ':')
			b = appendString(b, r.PropsStr[k])
		}
		b = append(b, '}')
	}
	if len(r.PropsNum) > 0 {
		b = append(b, `,"props_num":{`...)
		for i, k := range slices.Sorted(mapKeys(r.PropsNum)) {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendString(b, k)
			b = append(b, ':')
			var err error
			if b, err = appendFloat(b, r.PropsNum[k]); err != nil {
				return nil, err
			}
		}
		b = append(b, '}')
	}
	return append(b, '}'), nil
}

func mapKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func appendOptional(b []byte, key, v string) []byte {
	if v == "" {
		return b
	}
	return appendString(append(b, key...), v)
}

var errUnsupportedFloat = errors.New("event: NaN or Inf has no JSON form")

// appendFloat formats like encoding/json: the shortest representation, exponent form
// below 1e-6 and from 1e21, and "e-7" rather than "e-07".
func appendFloat(b []byte, f float64) ([]byte, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil, errUnsupportedFloat
	}
	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	b = strconv.AppendFloat(b, f, format, -1, 64)
	if format == 'e' {
		if n := len(b); n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	return b, nil
}

const hexDigits = "0123456789abcdef"

// appendString quotes s the way encoding/json does by default: ", \ and control
// characters escaped, <, > and & escaped for HTML, invalid UTF-8 replaced by U+FFFD,
// U+2028 and U+2029 escaped for JavaScript.
func appendString(b []byte, s string) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' && c != '<' && c != '>' && c != '&' {
				i++
				continue
			}
			b = append(b, s[start:i]...)
			switch c {
			case '"', '\\':
				b = append(b, '\\', c)
			case '\b':
				b = append(b, '\\', 'b')
			case '\f':
				b = append(b, '\\', 'f')
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, s[start:i]...)
			b = append(b, `�`...)
			i += size
			start = i
			continue
		}
		if r == ' ' || r == ' ' {
			b = append(b, s[start:i]...)
			b = append(b, '\\', 'u', '2', '0', '2', hexDigits[r&0xf])
			i += size
			start = i
			continue
		}
		i += size
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}

// AppendKey is Key into a buffer: the partition key "project:user", or the chat for
// events without a user, so a user's events stay in order on one partition.
func AppendKey(b []byte, projectID uint32, userID uint64, chatID int64) []byte {
	b = strconv.AppendUint(b, uint64(projectID), 10)
	b = append(b, ':')
	if userID == 0 && chatID != 0 {
		return strconv.AppendInt(b, chatID, 10)
	}
	return strconv.AppendUint(b, userID, 10)
}
