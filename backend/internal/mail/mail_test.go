package mail

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeSMTP accepts one message and records the envelope sender and the data.
func fakeSMTP(t *testing.T) (addr string, got chan [2]string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	got = make(chan [2]string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		reply := func(s string) { conn.Write([]byte(s + "\r\n")) }
		reply("220 fake")
		var from string
		var data strings.Builder
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			cmd := strings.ToUpper(strings.TrimSpace(line))
			switch {
			case strings.HasPrefix(cmd, "EHLO"), strings.HasPrefix(cmd, "HELO"):
				reply("250 fake")
			case strings.HasPrefix(cmd, "MAIL FROM:"):
				from = strings.TrimSpace(line)[len("MAIL FROM:"):]
				if !strings.HasPrefix(from, "<") || strings.Contains(from, " ") {
					reply("501 invalid FROM parameter")
					continue
				}
				reply("250 ok")
			case strings.HasPrefix(cmd, "RCPT TO:"):
				reply("250 ok")
			case cmd == "DATA":
				reply("354 go")
				for {
					l, err := r.ReadString('\n')
					if err != nil || l == ".\r\n" {
						break
					}
					data.WriteString(l)
				}
				reply("250 queued")
				got <- [2]string{from, data.String()}
			case cmd == "QUIT":
				reply("221 bye")
				return
			default:
				reply("250 ok")
			}
		}
	}()
	return ln.Addr().String(), got
}

func TestSMTPSend(t *testing.T) {
	addr, got := fakeSMTP(t)
	m := SMTP{Addr: addr, From: "Botsight <no-reply@botsight.ru>"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := m.Send(ctx, Message{To: "ivan@example.ru", Subject: "Сброс пароля", Body: "Ссылка:\nhttps://x"})
	if err != nil {
		t.Fatal(err)
	}
	msg := <-got
	if msg[0] != "<no-reply@botsight.ru>" {
		t.Fatalf("envelope sender %q", msg[0])
	}
	for _, want := range []string{"From: \"Botsight\" <no-reply@botsight.ru>\r\n", "To: ivan@example.ru\r\n", "Subject: =?utf-8?q?", "Ссылка:\r\nhttps://x"} {
		if !strings.Contains(msg[1], want) {
			t.Errorf("message has no %q:\n%s", want, msg[1])
		}
	}
}

func TestSMTPInvalidFrom(t *testing.T) {
	if err := (SMTP{Addr: "127.0.0.1:1", From: "not an address"}).Send(context.Background(), Message{To: "a@b.ru"}); err == nil {
		t.Fatal("invalid From accepted")
	}
}
