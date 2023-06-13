package cli

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"

	"github.com/adrg/xdg"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/cobra"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/oci"

	"github.com/joelanford/olm-oci/internal/inspect"
	"github.com/joelanford/olm-oci/internal/util"
)

func NewInspectCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <ociRef>",
		Short: "Recursively inspect an OCI reference (fetching from the remote repository as necessary)",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			src, _, desc, err := util.ResolveNameAndReference(cmd.Context(), args[0])
			if err != nil {
				log.Fatal(err)
			}
			storeDir := filepath.Join(xdg.CacheHome, "olm-oci", "store")
			dst, err := oci.NewWithContext(cmd.Context(), storeDir)
			if err != nil {
				log.Fatal(err)
			}
			if err := oras.ExtendedCopyGraph(cmd.Context(), src, dst, *desc, oras.ExtendedCopyGraphOptions{
				CopyGraphOptions: oras.CopyGraphOptions{
					Concurrency: 8,
					OnCopySkipped: func(ctx context.Context, desc ocispec.Descriptor) error {
						log.Printf("skipping copy of %s", desc.Digest)
						return nil
					},
					PreCopy: func(ctx context.Context, desc ocispec.Descriptor) error {
						log.Printf("starting to copy %s", desc.Digest)
						return nil
					},
					PostCopy: func(ctx context.Context, desc ocispec.Descriptor) error {
						log.Printf("finished copying %s", desc.Digest)
						return nil
					},
				},
			}); err != nil {
				log.Fatal(err)
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
