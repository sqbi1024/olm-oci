package cli

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/adrg/xdg"
	"github.com/spf13/cobra"
	"oras.land/oras-go/v2/content/oci"

	"github.com/joelanford/olm-oci/pkg/client"
	"github.com/joelanford/olm-oci/pkg/inspect"
	"github.com/joelanford/olm-oci/pkg/remote"
)

func NewInspectCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <ociRef>",
		Short: "Recursively inspect an OCI reference (fetching from the remote repository as necessary)",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			src, _, desc, err := remote.ResolveNameAndReference(cmd.Context(), args[0])
			if err != nil {
				log.Fatal(err)
			}

			storeDir := filepath.Join(xdg.CacheHome, "olm-oci", "store")
			dst, err := oci.NewWithContext(cmd.Context(), storeDir)
			if err != nil {
				log.Fatal(err)
			}

			if err := client.CopyGraphWithProgress(cmd.Context(), src, dst, *desc); err != nil {
				log.Fatalf("copying to local store: %v", err)
			}

			fileSrc := dst
			if err := inspect.Inspect(cmd.Context(), fileSrc, *desc); err != nil {
				if errors.Is(err, context.Canceled) {
					os.Exit(1)
				}
				log.Fatal(err)
			}
		},
	}
}
