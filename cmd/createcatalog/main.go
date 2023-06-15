package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/spf13/cobra"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content/oci"

	pkg "github.com/joelanford/olm-oci/api/v1"
	"github.com/joelanford/olm-oci/pkg/client"
	"github.com/joelanford/olm-oci/pkg/fetch"
	"github.com/joelanford/olm-oci/pkg/inspect"
	"github.com/joelanford/olm-oci/pkg/tar"
)

func main() {
	cmd := cobra.Command{
		Use:   "createcatalog <directory> <outputFile>",
		Short: "Build an OCI archive for a catalog from a directory of OCI archive bundles",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			inputDirectory, outputFile := args[0], args[1]
			if err := run(cmd.Context(), inputDirectory, outputFile); err != nil {
				log.Fatal(err)
			}
		},
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer cancel()
	_ = cmd.ExecuteContext(ctx)
}

func run(ctx context.Context, bundleDir, outputFile string) error {
	if _, err := os.Stat(outputFile); err == nil {
		return fmt.Errorf("output file already exists: %s", outputFile)
	}
	tmpDir, err := os.MkdirTemp("", "createcatalog-")
	if err != nil {
		return fmt.Errorf("create temp directory for OCI catalog: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	catalogStore, err := oci.NewWithContext(ctx, tmpDir)
	if err != nil {
		return fmt.Errorf("initialize OCI catalog: %v", err)
	}

	bundlesByPackage := map[string][]pkg.Bundle{}

	if err := filepath.Walk(bundleDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".oci.tar") {
			return nil
		}

		bundleStore, err := oci.NewFromTar(ctx, path)
		if err != nil {
			return err
		}

		desc, err := bundleStore.Resolve(ctx, "bundle")
		if err != nil {
			return err
		}

		if desc.MediaType != ocispec.MediaTypeArtifactManifest {
			return fmt.Errorf("unexpected media type %q, expected %q", desc.MediaType, ocispec.MediaTypeArtifactManifest)
		}

		rc, err := bundleStore.Fetch(ctx, desc)
		if err != nil {
			return err
		}
		defer rc.Close()
		bArt, err := inspect.DecodeArtifact(rc)
		if err != nil {
			return err
		}

		bundle, err := fetch.FetchBundle(ctx, bundleStore, bArt)
		if err != nil {
			return err
		}
		bundlesByPackage[bundle.Metadata.Package] = append(bundlesByPackage[bundle.Metadata.Package], *bundle)

		if err := oras.CopyGraph(ctx, bundleStore, catalogStore, desc, oras.DefaultCopyGraphOptions); err != nil {
			return err
		}

		tag := fmt.Sprintf("%s-%s-%d", bundle.Metadata.Package, bundle.Metadata.Version, bundle.Metadata.Release)
		if err := catalogStore.Tag(ctx, desc, tag); err != nil {
			return err
		}
		fmt.Printf("copied bundle %s to catalog at tag %s\n", path, tag)

		return nil
	}); err != nil {
		return err
	}

	packages := make([]pkg.Package, 0, len(bundlesByPackage))
	for pkgName, bundles := range bundlesByPackage {
		p := pkg.Package{
			Metadata: pkg.PackageMetadata{
				Name:        pkgName,
				DisplayName: pkgName,
			},
			Channels: []pkg.Channel{{
				Metadata: pkg.ChannelMetadata{
					Name: "",
				},
				Bundles: bundles,
			}},
		}
		packageDesc, err := client.Push(ctx, p, catalogStore)
		if err != nil {
			return err
		}
		if err := catalogStore.Tag(ctx, packageDesc, pkgName); err != nil {
			return err
		}
		packages = append(packages, p)
	}

	catalogDesc, err := client.Push(ctx, &pkg.Catalog{Packages: packages}, catalogStore)
	if err != nil {
		return err
	}

	tag := "catalog"
	if err := catalogStore.Tag(ctx, catalogDesc, tag); err != nil {
		return err
	}

	of, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("create output file: %v", err)
	}
	defer of.Close()
	if err := tar.WriteFS(os.DirFS(tmpDir), of); err != nil {
		return fmt.Errorf("write output file: %v", err)
	}
	fmt.Printf("Digest: %s@%s\n", outputFile, catalogDesc.Digest.String())
	fmt.Printf("Tag: %s:%s\n", outputFile, tag)

	return nil
}
