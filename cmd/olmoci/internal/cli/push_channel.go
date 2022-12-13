package cli

import (
	"log"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/internal/push"
	"github.com/joelanford/olm-oci/internal/remote"
)

func NewPushChannelCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "channel <channelDir> <targetRepo>",
		Short: "Push an OLM OCI channel artifact to a registry.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			channelDir := args[0]
			targetRepo := args[1]
			repo, err := remote.NewRepository(targetRepo)
			if err != nil {
				log.Fatal(err)
			}
			_, size, err := push.Channel(cmd.Context(), repo, channelDir, filepath.Base(channelDir))
			log.Printf("total bytes pushed: %d", size)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
}
