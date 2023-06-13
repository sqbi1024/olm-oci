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
		NewPushPackageCommand(),
		NewPushBundleCommand(),
	)
	return cmd
}
