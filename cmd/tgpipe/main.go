// Package main is the tgpipe CLI entry point. Subcommands live in sibling
// files (cmd_auth, cmd_crawl, cmd_run, cmd_stats, cmd_retry, cmd_reset).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// version is overridden at build time via:
//
//	go build -ldflags "-X main.version=$(git describe --tags --always)" ./cmd/tgpipe
var version = "dev"

var (
	cfgPath      string
	debugPprof   bool
	logLevelFlag string // overrides config.logging.level when non-empty
)

var rootCmd = &cobra.Command{
	Use:     "tgpipe",
	Short:   "Telegram bulk downloader & republisher",
	Version: version,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgPath, "config", "config.yaml", "path to config YAML")
	rootCmd.PersistentFlags().BoolVar(&debugPprof, "debug-pprof", false, "enable pprof on 127.0.0.1:6060")
	rootCmd.PersistentFlags().StringVar(&logLevelFlag, "log-level", "", "override logging.level (debug|info|warn|error)")
	rootCmd.AddCommand(authCmd, crawlCmd, runCmd, statsCmd, retryCmd, resetCmd)
}

// resolveLogLevel returns the effective log level: CLI flag wins over config.
func resolveLogLevel(fromConfig string) string {
	if logLevelFlag != "" {
		return logLevelFlag
	}
	return fromConfig
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
