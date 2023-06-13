package cli

import (
	"github.com/spf13/cobra"
)

func NewBuildCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build OLM OCI artifacts as local OCI archive files",
	}
	cmd.AddCommand(
		NewBuildBundleCommand(),
		NewBuildCatalogCommand(),
	)
	return cmd
}
