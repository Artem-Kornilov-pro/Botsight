package botproxy

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Event is one event in the SDK wire format (Botsight.md, 9): the proxy is a server-side
// SDK and sends the same batches to the collector.
type Event struct {
	EventID      string    `json:"event_id"`
	Type         string    `json:"type"`
	Name         string    `json:"name"`
	TS           string    `json:"ts"`
	BotID        int64     `json:"bot_id"`
	UserID       int64     `json:"user_id"`
	ChatID       int64     `json:"chat_id"`
	ChatType     string    `json:"chat_type,omitempty"`
	ContentType  string    `json:"content_type,omitempty"`
	Command      string    `json:"command,omitempty"`
	StartParam   string    `json:"start_param,omitempty"`
	CallbackData string    `json:"callback_data,omitempty"`
	MemberStatus string    `json:"member_status,omitempty"`
	TextLen      int       `json:"text_len,omitempty"`
	User         *wireUser `json:"user,omitempty"`
	// The proxy cannot see the handlers: an update counts as handled, and the time is
	// the Bot API call for outgoing events only.
	Handled    bool    `json:"handled"`
	OK         bool    `json:"ok"`
	DurationMS int64   `json:"duration_ms"`
	ErrorCode  int     `json:"error_code"`
	Error      *string `json:"error"`
}

type wireUser struct {
	Lang        string `json:"lang"`
	IsPremium   bool   `json:"is_premium"`
	HasUsername bool   `json:"has_username"`
}

func newEvent(typ, name string, botID int64, at time.Time) Event {
	id, err := uuid.NewV7()
	if err != nil {
		id = uuid.New()
	}
	return Event{EventID: id.String(), Type: typ, Name: name, BotID: botID, TS: at.UTC().Format("2006-01-02T15:04:05.000Z")}
}

// --- incoming updates ---

type tgUser struct {
	ID           int64  `json:"id"`
	Username     string `json:"username"`
	LanguageCode string `json:"language_code"`
	IsPremium    bool   `json:"is_premium"`
}

type tgChat struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
}

// payload has the fields of every kind of update that the events need: a message,
// a button press, a status change in a chat, a poll answer.
type payload struct {
	From          *tgUser `json:"from"`
	User          *tgUser `json:"user"` // poll_answer
	Chat          *tgChat `json:"chat"`
	Message       *tgMsg  `json:"message"` // callback_query
	Data          string  `json:"data"`
	Text          string  `json:"text"`
	Caption       string  `json:"caption"`
	NewChatMember *struct {
		Status string `json:"status"`
	} `json:"new_chat_member"`
}

type tgMsg struct {
	Chat *tgChat `json:"chat"`
}

// contentTypes are the message fields that say what was sent, in aiogram's names.
var contentTypes = []string{
	"text", "photo", "video", "document", "sticker", "voice", "audio", "animation", "video_note",
	"contact", "location", "venue", "poll", "dice", "game", "invoice", "successful_payment",
	"story", "new_chat_members", "left_chat_member", "web_app_data", "users_shared", "chat_shared",
}

// UpdateEvent turns one Update object into an update event; ok is false for JSON that
// is not an update. id is update_id for deduplication.
func UpdateEvent(raw json.RawMessage, botID int64, at time.Time) (ev Event, id int64, ok bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return ev, 0, false
	}
	if json.Unmarshal(fields["update_id"], &id) != nil {
		return ev, 0, false
	}
	// an update has update_id and exactly one more field: its kind
	name := ""
	for k := range fields {
		if k != "update_id" {
			name = k
			break
		}
	}
	if name == "" {
		return ev, 0, false
	}
	ev = newEvent("update", name, botID, at)
	ev.Handled, ev.OK = true, true

	var p payload
	_ = json.Unmarshal(fields[name], &p)
	user := p.From
	if user == nil {
		user = p.User
	}
	if user != nil {
		ev.UserID = user.ID
		ev.User = &wireUser{Lang: user.LanguageCode, IsPremium: user.IsPremium, HasUsername: user.Username != ""}
	}
	chat := p.Chat
	if chat == nil && p.Message != nil {
		chat = p.Message.Chat
	}
	if chat != nil {
		ev.ChatID, ev.ChatType = chat.ID, chat.Type
	}

	switch name {
	case "message", "edited_message", "channel_post", "edited_channel_post", "business_message":
		var msg map[string]json.RawMessage
		_ = json.Unmarshal(fields[name], &msg)
		for _, ct := range contentTypes {
			if _, has := msg[ct]; has {
				ev.ContentType = ct
				break
			}
		}
		text := p.Text
		if text == "" {
			text = p.Caption
		}
		ev.TextLen = utf8.RuneCountInString(text)
		if p.Text != "" && strings.HasPrefix(p.Text, "/") {
			ev.Command, ev.StartParam = parseCommand(p.Text)
		}
	case "callback_query":
		ev.CallbackData = p.Data
	case "my_chat_member", "chat_member":
		// "kicked" in a private chat means the user blocked the bot (Botsight.md, 8.6)
		if p.NewChatMember != nil {
			ev.MemberStatus = p.NewChatMember.Status
		}
	}
	return ev, id, true
}

// parseCommand works like the Python SDK: "/start@bot vk_ads" -> start, vk_ads.
func parseCommand(text string) (command, startParam string) {
	head, rest, _ := strings.Cut(strings.TrimSpace(text), " ")
	command, _, _ = strings.Cut(strings.TrimPrefix(head, "/"), "@")
	command = truncate(strings.ToLower(command), 64)
	if command == "start" {
		startParam = truncate(strings.TrimSpace(rest), 64)
	}
	return command, startParam
}

func truncate(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	r := []rune(s)
	return string(r[:n])
}

// --- Bot API answers ---

// apiResponse is the envelope of every Bot API answer.
type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result"`
	ErrorCode   int             `json:"error_code"`
	Description string          `json:"description"`
	Parameters  *struct {
		MigrateToChatID int64 `json:"migrate_to_chat_id"`
		RetryAfter      int   `json:"retry_after"`
	} `json:"parameters"`
}

// NormalizeError maps a failed answer to (code, error) with the table of Botsight.md, 8.5,
// so that events from the proxy and from the SDK are counted together.
func NormalizeError(code int, description string, migrate bool) (int, string) {
	d := strings.ToLower(description)
	switch {
	case code == 403 && strings.Contains(d, "bot was blocked by the user"):
		return 403, "bot_blocked"
	case code == 403 && strings.Contains(d, "user is deactivated"):
		return 403, "user_deactivated"
	case code == 403:
		return 403, "forbidden"
	case code == 429:
		return 429, "rate_limited"
	case code == 400 && migrate:
		return 400, "migrated"
	case code == 400 && strings.Contains(d, "message is not modified"):
		return 400, "not_modified"
	case code == 400:
		return 400, "bad_request"
	case code == 401:
		return 401, "unauthorized"
	case code == 404:
		return 404, "not_found"
	case code == 409:
		return 409, "conflict"
	case code == 413:
		return 413, "entity_too_large"
	case code >= 500:
		return 500, "server_error"
	}
	return 0, "api_error"
}

// resultChatID is the chat of a sent message: result is a Message, or an array of them
// for sendMediaGroup. Other results have no chat.
func resultChatID(result json.RawMessage) int64 {
	var one tgMsg
	if json.Unmarshal(result, &one) == nil && one.Chat != nil {
		return one.Chat.ID
	}
	var many []tgMsg
	if json.Unmarshal(result, &many) == nil && len(many) > 0 && many[0].Chat != nil {
		return many[0].Chat.ID
	}
	return 0
}

// parseToken gives the bot id, the part of the token before the colon.
func parseToken(token string) (int64, bool) {
	head, secret, ok := strings.Cut(token, ":")
	id, err := strconv.ParseInt(head, 10, 64)
	return id, ok && err == nil && id > 0 && secret != ""
}

func strPtr(s string) *string { return &s }
