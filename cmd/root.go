package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	cfgFile string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "migration-event-streamer",
	Short: "Stream events from assisted migration to elastic",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.migration-event-streamer.yaml)")

	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
