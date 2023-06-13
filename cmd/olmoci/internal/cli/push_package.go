package cli

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/internal/client"
	"github.com/joelanford/olm-oci/internal/pkg"
)

func NewPushPackageCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "package <packageDir> <targetRepo>",
		Short: "Push an OLM OCI package artifact to a registry.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			packageDir := args[0]
			targetRepo := args[1]

			p, err := pkg.LoadPackage(packageDir)
			if err != nil {
				log.Fatalf("load package: %v", err)
			}

			if _, err := client.DefaultClient.Push(cmd.Context(), p, targetRepo); err != nil {
				log.Fatal(err)
			}
		},
	}
}
