package cli

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/internal/copy"
	"github.com/joelanford/olm-oci/internal/remote"
)

func NewCopyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "copy <srcRef> <destRepo>",
		Short: "Recursively copy an OCI artifact to a destination repository.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			srcRefStr := args[0]
			destRepoName := args[1]

			destRepo, err := remote.NewRepository(destRepoName)
			if err != nil {
				log.Fatal(err)
			}

			_, size, err := copy.Reference(cmd.Context(), destRepo, srcRefStr)
			log.Printf("total bytes pushed: %d", size)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
}
