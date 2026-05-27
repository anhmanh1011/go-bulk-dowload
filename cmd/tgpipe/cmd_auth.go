package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/manh/tgpipe/internal/config"
)

var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Interactive MTProto login — writes session file",
	RunE: func(cmd *cobra.Command, _ []string) error {
		ctx := cmd.Context()
		cfg, err := config.Load(cfgPath)
		if err != nil {
			return err
		}
		if cfg.Account.APIID == 0 || cfg.Account.APIHash == "" {
			return fmt.Errorf("account.api_id and account.api_hash must be set in %s", cfgPath)
		}
		if dir := filepath.Dir(cfg.Account.SessionFile); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return err
			}
		}
		client := telegram.NewClient(cfg.Account.APIID, cfg.Account.APIHash, telegram.Options{
			SessionStorage: &session.FileStorage{Path: cfg.Account.SessionFile},
		})
		return client.Run(ctx, func(ctx context.Context) error {
			flow := auth.NewFlow(termAuth{}, auth.SendCodeOptions{})
			if err := client.Auth().IfNecessary(ctx, flow); err != nil {
				return err
			}
			self, err := client.Self(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("authenticated as @%s (id=%d) — session written to %s\n",
				self.Username, self.ID, cfg.Account.SessionFile)
			return nil
		})
	},
}

// termAuth implements auth.UserAuthenticator by prompting on stdin/stderr.
type termAuth struct{}

func (termAuth) Phone(_ context.Context) (string, error) {
	fmt.Fprint(os.Stderr, "Phone (+E.164, e.g. +84901234567): ")
	r := bufio.NewReader(os.Stdin)
	s, err := r.ReadString('\n')
	return strings.TrimSpace(s), err
}

func (termAuth) Password(_ context.Context) (string, error) {
	fmt.Fprint(os.Stderr, "2FA password: ")
	// int(os.Stdin.Fd()) is portable across Unix and Windows; syscall.Stdin
	// is a Handle (uintptr) on Windows and won't convert cleanly to int.
	pw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	return strings.TrimSpace(string(pw)), err
}

func (termAuth) Code(_ context.Context, _ *tg.AuthSentCode) (string, error) {
	fmt.Fprint(os.Stderr, "Code: ")
	r := bufio.NewReader(os.Stdin)
	s, err := r.ReadString('\n')
	return strings.TrimSpace(s), err
}

func (termAuth) AcceptTermsOfService(_ context.Context, tos tg.HelpTermsOfService) error {
	fmt.Fprintln(os.Stderr, "ToS:", tos.Text)
	return nil
}

func (termAuth) SignUp(_ context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, fmt.Errorf("sign-up not supported; use an existing account")
}
