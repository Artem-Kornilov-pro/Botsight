package gotelegrambot_test

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	botsight "github.com/Artem-Kornilov-pro/Botsight/sdk/go"
	"github.com/Artem-Kornilov-pro/Botsight/sdk/go/gotelegrambot"
)

// A bot on go-telegram/bot with Botsight: three options to bot.New.
func Example() {
	bs, err := botsight.New(os.Getenv("BOTSIGHT_KEY"), gotelegrambot.Framework())
	if err != nil {
		panic(err)
	}
	defer bs.Close(context.Background()) // sends what is queued

	b, err := bot.New(os.Getenv("BOT_TOKEN"),
		bot.WithHTTPClient(time.Minute, bs.HTTPClient()),
		bot.WithMiddlewares(gotelegrambot.Middleware(bs)),
		bot.WithDefaultHandler(gotelegrambot.Fallback(bs, nil)),
	)
	if err != nil {
		panic(err)
	}
	b.RegisterHandler(bot.HandlerTypeMessageText, "/buy", bot.MatchTypeExact,
		gotelegrambot.Named("buy", func(ctx context.Context, b *bot.Bot, u *models.Update) {
			bs.Track("purchase", u.Message.From.ID, map[string]any{"amount": 990})
			_, _ = b.SendMessage(ctx, &bot.SendMessageParams{ChatID: u.Message.Chat.ID, Text: "Спасибо!"})
		}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	b.Start(ctx)
}
