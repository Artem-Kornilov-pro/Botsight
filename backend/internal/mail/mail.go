// Package mail sends service emails: email confirmation and password reset.
package mail

import (
	"context"
	"fmt"
	"log/slog"
	"mime"
	"net"
	netmail "net/mail"
	"net/smtp"
	"strings"
	"time"
)

type Message struct {
	To      string
	Subject string
	Body    string // plain text
}

type Mailer interface {
	Send(ctx context.Context, msg Message) error
}

// SMTP sends through an SMTP server: Mailpit in development, a provider in production.
type SMTP struct {
	Addr     string // host:port
	From     string // "Botsight <no-reply@botsight.ru>" or a bare address
	Username string // optional; PLAIN auth requires TLS unless the host is localhost
	Password string
}

func (s SMTP) Send(ctx context.Context, msg Message) error {
	from, err := netmail.ParseAddress(s.From)
	if err != nil {
		return fmt.Errorf("mail: invalid From %q: %w", s.From, err)
	}
	var a smtp.Auth
	if s.Username != "" {
		host, _, err := net.SplitHostPort(s.Addr)
		if err != nil {
			return err
		}
		a = smtp.PlainAuth("", s.Username, s.Password, host)
	}
	done := make(chan error, 1)
	// The SMTP envelope takes the bare address; the header keeps the display name.
	go func() { done <- smtp.SendMail(s.Addr, a, from.Address, []string{msg.To}, render(from, msg)) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func render(from *netmail.Address, msg Message) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", from.String())
	fmt.Fprintf(&b, "To: %s\r\n", msg.To)
	fmt.Fprintf(&b, "Subject: %s\r\n", mime.QEncoding.Encode("utf-8", msg.Subject))
	fmt.Fprintf(&b, "Date: %s\r\n", time.Now().Format(time.RFC1123Z))
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: 8bit\r\n\r\n")
	b.WriteString(strings.ReplaceAll(msg.Body, "\n", "\r\n"))
	return []byte(b.String())
}

// Log writes emails to the log instead of sending them; for local runs without SMTP.
type Log struct {
	Logger *slog.Logger
}

func (l Log) Send(_ context.Context, msg Message) error {
	l.Logger.Info("email not sent (SMTP is not configured)", "to", msg.To, "subject", msg.Subject, "body", msg.Body)
	return nil
}
