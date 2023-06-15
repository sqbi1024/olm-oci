package cli

import (
	"context"
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"oras.land/oras-go/v2/content/oci"
	"oras.land/oras-go/v2/registry"

	"github.com/joelanford/olm-oci/pkg/client"
	"github.com/joelanford/olm-oci/pkg/remote"
)

func NewPushArchiveCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "archive <archive> <targetRepository>",
		Short: "Push an OLM OCI archive to a registry.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			archiveFile := args[0]
			targetRepo := args[1]

			if err := runPushArchive(cmd.Context(), archiveFile, targetRepo); err != nil {
				log.Fatal(err)
			}
		},
	}
}

func runPushArchive(ctx context.Context, archiveFile, targetRepo string) error {
	repo, _, err := remote.ParseNameAndReference(targetRepo)
	if err != nil {
		return fmt.Errorf("parse target reference: %v", err)
	}

	src, err := oci.NewFromTar(ctx, archiveFile)
	if err != nil {
		return fmt.Errorf("load archive: %v", err)
	}

	tags, err := registry.Tags(ctx, src)
	if err != nil {
		return fmt.Errorf("get tags: %v", err)
	}
	for _, tag := range tags {
		desc, err := src.Resolve(ctx, tag)
		if err != nil {
			return fmt.Errorf("resolve tag %s: %v", tag, err)
		}
		if err := client.CopyGraphWithProgress(ctx, src, repo, desc); err != nil {
			return fmt.Errorf("push archive: %v", err)
		}
		if err := repo.Tag(ctx, desc, tag); err != nil {
			return fmt.Errorf("tag archive: %v", err)
		}
	}
	return nil
}
