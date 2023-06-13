package cli

import (
	"github.com/spf13/cobra"
)

func NewSystemCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "system",
		Short: "Commands for managing olm-oci state on the local system",
	}
	cmd.AddCommand(
		NewSystemResetCommand(),
	)
	return cmd
}
