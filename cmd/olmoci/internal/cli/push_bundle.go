package cli

import (
	"log"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/internal/push"
	"github.com/joelanford/olm-oci/internal/remote"
)

func NewPushBundleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "bundle <bundleDir> <targetRepo>",
		Short: "Push an OLM OCI bundle artifact to a registry.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			bundleDir := args[0]
			targetRepo := args[1]
			repo, err := remote.NewRepository(targetRepo)
			if err != nil {
				log.Fatal(err)
			}
			_, size, err := push.Bundle(cmd.Context(), repo, bundleDir, filepath.Base(bundleDir))
			log.Printf("total bytes pushed: %d", size)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
}
