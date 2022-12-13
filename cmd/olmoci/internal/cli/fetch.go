package cli

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/internal/fetch"
)

func NewFetchCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "fetch <ociRef>",
		Short: "Recursively fetch an OCI reference and print its content and hierarchy.",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			ref := args[0]

			if err := fetch.Fetch(cmd.Context(), ref); err != nil {
				log.Fatal(err)
			}
		},
	}
}
