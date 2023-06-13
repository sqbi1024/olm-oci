package cli

import (
	"context"
	"io/fs"
	"log"
	"path/filepath"

	"github.com/spf13/cobra"
	"oras.land/oras-go/v2/content/oci"
)

func NewBuildCatalogCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "catalog <bundlesDir> <catalogFile>",
		Short: "Build OLM OCI catalog",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			bundlesDir := args[0]
			outputFile := args[1]
			if err := runBuildCatalog(cmd.Context(), bundlesDir, outputFile); err != nil {
				log.Fatal(err)
			}
		},
	}
	return cmd
}

func runBuildCatalog(ctx context.Context, bundlesDir, outputFile string) error {
	return filepath.Walk(bundlesDir, func(path string, d fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".tar" {
			return nil
		}
		s, err := oci.NewFromTar(ctx, path)
		if err != nil {
			return err
		}
		desc, err := s.Resolve(ctx, "bundle")
		if err != nil {
			return err
		}
		log.Printf("bundle: %s\n", desc.Digest)
		return nil
	})
	//b, err := pkg.LoadBundle(bundleDir)
	//if err != nil {
	//	return fmt.Errorf("load bundle: %v", err)
	//}
	//
	//if _, err := os.Stat(outputFile); err == nil {
	//	return fmt.Errorf("output file already exists: %s", outputFile)
	//}
	//
	//tmpDir, err := os.MkdirTemp("", "olmoci-build-bundle-")
	//if err != nil {
	//	return fmt.Errorf("create temp dir: %v", err)
	//}
	//defer os.RemoveAll(tmpDir)
	//
	//store, err := oci.NewWithContext(ctx, tmpDir)
	//if err != nil {
	//	return fmt.Errorf("create local bundle store: %v", err)
	//}
	//desc, err := client.Push(ctx, b, store)
	//if err != nil {
	//	return fmt.Errorf("build bundle: %v", err)
	//}
	//if err := store.Tag(ctx, *desc, "bundle"); err != nil {
	//	return fmt.Errorf("tag bundle: %v", err)
	//}
	//of, err := os.Create(outputFile)
	//if err != nil {
	//	return fmt.Errorf("create output file: %v", err)
	//}
	//defer of.Close()
	//if err := tar.WriteFS(os.DirFS(tmpDir), of); err != nil {
	//	return fmt.Errorf("write output file: %v", err)
	//}
	//return nil
}
