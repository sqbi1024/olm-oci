package cli

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/internal/copy"
)

func NewCopyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "copy <srcRef> <destRef>",
		Short: "Recursively copy an OCI artifact to a destination repository.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			srcRefStr := args[0]
			destRepoStr := args[1]

			_, size, err := copy.Reference(cmd.Context(), destRepoStr, srcRefStr)
			log.Printf("total bytes pushed: %d", size)
			if err != nil {
				log.Fatal(err)
			}
		},
	}
}
