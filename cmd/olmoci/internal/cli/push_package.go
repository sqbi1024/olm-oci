package cli

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/internal/push"
	"github.com/joelanford/olm-oci/internal/remote"
)

func NewPushPackageCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "package <packageDir> <targetRepo>",
		Short: "Push an OLM OCI package artifact to a registry.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			packageDir := args[0]
			targetRepo := args[1]
			repo, err := remote.NewRepository(targetRepo)
			if err != nil {
				log.Fatal(err)
			}
			_, size, err := push.Package(cmd.Context(), repo, packageDir)
			log.Printf("total bytes pushed: %d", size)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
}
