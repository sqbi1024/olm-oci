package cli

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/internal/client"
	"github.com/joelanford/olm-oci/internal/pkg"
)

func NewPushBundleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "bundle <bundleDir> <targetRepo>",
		Short: "Push an OLM OCI bundle artifact to a registry.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			bundleDir := args[0]
			targetRepo := args[1]

			b, err := pkg.LoadBundle(bundleDir)
			if err != nil {
				log.Fatalf("load bundle: %v", err)
			}

			if _, err := client.DefaultClient.Push(cmd.Context(), b, targetRepo); err != nil {
				log.Fatal(err)
			}
		},
	}
}
