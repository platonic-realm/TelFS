package tg

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"golang.org/x/term"
)

// Login runs the interactive MTProto auth flow (phone → code → 2FA) and
// persists the resulting session to disk. Safe to call when already
// authorized — it short-circuits via Auth().IfNecessary.
func (c *Client) Login(ctx context.Context) error {
	phone := strings.TrimSpace(c.cfg.Phone)
	if phone == "" {
		var err error
		phone, err = promptLine("Phone number (international, e.g. +14155552671): ")
		if err != nil {
			return err
		}
	}

	tgc := c.newTG()
	return tgc.Run(ctx, func(ctx context.Context) error {
		flow := auth.NewFlow(termAuth{phone: phone}, auth.SendCodeOptions{})
		if err := tgc.Auth().IfNecessary(ctx, flow); err != nil {
			return fmt.Errorf("auth: %w", err)
		}
		self, err := tgc.Self(ctx)
		if err != nil {
			return fmt.Errorf("self: %w", err)
		}
		uname := self.Username
		if uname == "" {
			uname = "(no username)"
		}
		fmt.Printf("Logged in as @%s (%s, id=%d). Session saved to %s\n",
			uname, fullName(self), self.ID, c.cfg.SessionPath())
		return nil
	})
}

func fullName(u *tg.User) string {
	name := strings.TrimSpace(u.FirstName + " " + u.LastName)
	if name == "" {
		return "(no name)"
	}
	return name
}

// termAuth satisfies gotd's auth.UserAuthenticator using the terminal.
type termAuth struct {
	phone string
}

func (a termAuth) Phone(_ context.Context) (string, error) { return a.phone, nil }

func (a termAuth) Code(_ context.Context, _ *tg.AuthSentCode) (string, error) {
	return promptLine("Login code: ")
}

func (a termAuth) Password(_ context.Context) (string, error) {
	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		fmt.Print("2FA password: ")
		b, err := term.ReadPassword(fd)
		fmt.Println()
		if err != nil {
			return "", fmt.Errorf("read password: %w", err)
		}
		return string(b), nil
	}
	// Non-tty stdin (e.g. piped from a FIFO): read a line normally.
	// No echo suppression is possible without a tty.
	return promptLine("2FA password: ")
}

func (a termAuth) AcceptTermsOfService(_ context.Context, tos tg.HelpTermsOfService) error {
	fmt.Println("Telegram Terms of Service:")
	fmt.Println(tos.Text)
	ok, err := promptLine("Accept? [y/N]: ")
	if err != nil {
		return err
	}
	if !strings.EqualFold(ok, "y") && !strings.EqualFold(ok, "yes") {
		return errors.New("ToS declined")
	}
	return nil
}

func (a termAuth) SignUp(_ context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New(
		"sign-up not supported — register the account in the Telegram app first")
}

func promptLine(prompt string) (string, error) {
	fmt.Print(prompt)
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}
