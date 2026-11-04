// Package gotelegrambot connects Botsight to bots on github.com/go-telegram/bot.
//
//	bs, _ := botsight.New(os.Getenv("BOTSIGHT_KEY"))
//	defer bs.Close(context.Background())
//	b, _ := bot.New(token,
//		bot.WithHTTPClient(time.Minute, bs.HTTPClient()),       // Bot API calls
//		bot.WithMiddlewares(gotelegrambot.Middleware(bs)),      // updates and handler time
//		bot.WithDefaultHandler(gotelegrambot.Fallback(bs, nil)), // updates no handler took
//	)
//	b.RegisterHandler(bot.HandlerTypeMessageText, "/start", bot.MatchTypePrefix,
//		gotelegrambot.Named("start", startHandler))              // slow handlers by name
//
// The middleware sees every update, polling or webhook, so it reports the updates and
// the HTTP client reports only the calls the bot makes.
package gotelegrambot

import (
	"context"
	"encoding/json"
	"runtime/debug"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	botsight "github.com/Artem-Kornilov-pro/Botsight/sdk/go"
)

type stateKey struct{}

// state is shared by the middleware and the handler wrappers of one update.
type state struct {
	unhandled bool
	handler   string
}

func frameworkVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, m := range info.Deps {
			if m.Path == "github.com/go-telegram/bot" {
				return m.Version
			}
		}
	}
	return "unknown"
}

// Framework is the option that names the library in batches; pass it to botsight.New.
func Framework() botsight.Option {
	return botsight.WithFramework("go-telegram/bot", frameworkVersion())
}

// Middleware reports every update with the time its handler took. A handler that panics
// is recorded as a failed step, and the panic goes on as without Botsight.
func Middleware(c *botsight.Client) bot.Middleware {
	c.UseAdapter()
	return func(next bot.HandlerFunc) bot.HandlerFunc {
		return func(ctx context.Context, b *bot.Bot, u *models.Update) {
			st := &state{}
			ctx = context.WithValue(ctx, stateKey{}, st)
			start := time.Now()
			defer func() {
				d := time.Since(start)
				failure := recover()
				record(c, b, u, st, d, failure)
				if failure != nil {
					panic(failure)
				}
			}()
			next(ctx, b, u)
		}
	}
}

func record(c *botsight.Client, b *bot.Bot, u *models.Update, st *state, d time.Duration, failure any) {
	defer func() { _ = recover() }() // analytics never adds a panic of its own
	id := b.ID()
	if raw, err := json.Marshal(u); err == nil {
		c.Update(id, raw, !st.unhandled, d)
	}
	if st.handler != "" || failure != nil {
		kind, user, chat := describe(u)
		name := st.handler
		if name == "" {
			name = "unnamed"
		}
		c.Step(id, kind, name, user, chat, d, failure)
	}
}

// Fallback wraps the default handler (nil is fine): updates that reach it count as
// unhandled, the share the dashboard shows as "без обработчика".
func Fallback(c *botsight.Client, h bot.HandlerFunc) bot.HandlerFunc {
	return func(ctx context.Context, b *bot.Bot, u *models.Update) {
		if st, ok := ctx.Value(stateKey{}).(*state); ok {
			st.unhandled = true
		}
		if h != nil {
			h(ctx, b, u)
		}
	}
}

// Named gives a handler a name for step events: the dashboard lists slow and failing
// handlers by it.
func Named(name string, h bot.HandlerFunc) bot.HandlerFunc {
	return func(ctx context.Context, b *bot.Bot, u *models.Update) {
		if st, ok := ctx.Value(stateKey{}).(*state); ok {
			st.handler = name
		}
		h(ctx, b, u)
	}
}

// describe is the kind of update, its user and chat, for step events.
func describe(u *models.Update) (kind string, user, chat int64) {
	switch {
	case u.Message != nil:
		kind, chat = "message", u.Message.Chat.ID
		if u.Message.From != nil {
			user = u.Message.From.ID
		}
	case u.CallbackQuery != nil:
		kind, user = "callback_query", u.CallbackQuery.From.ID
		if m := u.CallbackQuery.Message.Message; m != nil {
			chat = m.Chat.ID
		}
	case u.PreCheckoutQuery != nil:
		kind = "pre_checkout_query"
		if u.PreCheckoutQuery.From != nil {
			user = u.PreCheckoutQuery.From.ID
		}
	default:
		kind = "update"
	}
	return kind, user, chat
}
