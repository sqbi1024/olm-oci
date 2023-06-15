package cli

import (
	"github.com/spf13/cobra"
)

func NewPushCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "push",
		Short: "Push OLM OCI artifacts to a registry.",
	}
	cmd.AddCommand(
		NewPushArchiveCommand(),
		NewPushPackageCommand(),
		NewPushBundleCommand(),
	)
	return cmd
}
