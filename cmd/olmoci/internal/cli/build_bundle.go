package cli

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"oras.land/oras-go/v2/content/oci"

	pkg "github.com/joelanford/olm-oci/api/v1"
	"github.com/joelanford/olm-oci/pkg/client"
	"github.com/joelanford/olm-oci/pkg/tar"
)

func NewBuildBundleCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "bundle <bundleDir> <outputFile>",
		Short: "Build OLM OCI bundle",
		Run: func(cmd *cobra.Command, args []string) {
			bundleDir := args[0]
			outputFile := args[1]
			if err := runBuildBundle(cmd.Context(), bundleDir, outputFile); err != nil {
				log.Fatal(err)
			}
		},
	}
	return cmd
}

func runBuildBundle(ctx context.Context, bundleDir, outputFile string) error {
	b, err := pkg.LoadBundle(bundleDir)
	if err != nil {
		return fmt.Errorf("load bundle: %v", err)
	}

	if _, err := os.Stat(outputFile); err == nil {
		return fmt.Errorf("output file already exists: %s", outputFile)
	}

	tmpDir, err := os.MkdirTemp("", "olmoci-build-bundle-")
	if err != nil {
		return fmt.Errorf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := oci.NewWithContext(ctx, tmpDir)
	if err != nil {
		return fmt.Errorf("create local bundle store: %v", err)
	}
	desc, err := client.Push(ctx, b, store)
	if err != nil {
		return fmt.Errorf("build bundle: %v", err)
	}
	if err := store.Tag(ctx, desc, "bundle"); err != nil {
		return fmt.Errorf("tag bundle: %v", err)
	}
	of, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create output file: %v", err)
	}
	defer of.Close()
	if err := tar.WriteFS(os.DirFS(tmpDir), of); err != nil {
		return fmt.Errorf("write output file: %v", err)
	}
	fmt.Printf("Digest: %s@%s\n", outputFile, desc.Digest.String())
	fmt.Printf("Tag: %s:bundle\n", outputFile)
	return nil
}
