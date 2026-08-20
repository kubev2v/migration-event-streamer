package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/kubev2v/migration-event-streamer/cmd"
	"github.com/kubev2v/migration-event-streamer/internal/config"
	"github.com/kubev2v/migration-event-streamer/internal/logger"
)

var (
	version   = "v0.0.0"
	gitCommit = "unknown"
)

func main() {
	cfg := config.NewConfigurationWithOptionsAndDefaults()

	rootCmd := &cobra.Command{
		Use:   "migration-event-streamer",
		Short: "Stream events from assisted migration to elastic",
	}

	rootCmd.PersistentFlags().StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "Log format: console or json")
	rootCmd.PersistentFlags().StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug, info, warn, error")

	rootCmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
		l := logger.SetupLogger(cfg.LogFormat, cfg.LogLevel)
		zap.ReplaceGlobals(l)
	}

	rootCmd.AddCommand(cmd.NewRunCommand(cfg, version, gitCommit))

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
