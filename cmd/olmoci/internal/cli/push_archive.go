package cli

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/containers/image/v5/docker/reference"
	"github.com/opencontainers/go-digest"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
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
			archiveRefStr := args[0]
			targetRefStr := args[1]

			if err := runPushArchive(cmd.Context(), archiveRefStr, targetRefStr); err != nil {
				log.Fatal(err)
			}
		},
	}
}

func runPushArchive(ctx context.Context, archiveRefStr, targetRefStr string) error {
	archiveRef, err := reference.Parse(archiveRefStr)
	if err != nil {
		return fmt.Errorf("parse artifact reference: %v", err)
	}
	targetRef, err := reference.Parse(targetRefStr)
	if err != nil {
		return fmt.Errorf("parse target reference: %v", err)
	}
	archiveRefNamed, ok := archiveRef.(reference.Named)
	if !ok {
		return fmt.Errorf("archive reference is not named")
	}
	targetRefNamed, ok := targetRef.(reference.Named)
	if !ok {
		return fmt.Errorf("target reference is not named")
	}
	archiveTagOrDig, archiveTagDigErr := remote.TagOrDigest(archiveRefNamed)
	_, targetTagDigErr := remote.TagOrDigest(targetRefNamed)
	if archiveTagDigErr != targetTagDigErr {
		return fmt.Errorf("archive and target reference must be both repository references or specific descriptor references (tag/digest)")
	}

	if s, err := os.Stat(archiveRefNamed.Name()); err != nil || s.IsDir() {
		return fmt.Errorf("archive reference must be a file")
	}

	srcRepo, err := oci.NewFromTar(ctx, archiveRefNamed.Name())
	if err != nil {
		return fmt.Errorf("load archive: %v", err)
	}

	targetRepo, err := remote.NewRepository(targetRefNamed.Name())
	if err != nil {
		return fmt.Errorf("create target repository client: %v", err)
	}

	if archiveTagDigErr == nil {
		desc, err := srcRepo.Resolve(ctx, archiveTagOrDig)
		if err != nil {
			return fmt.Errorf("resolve archive reference: %v", err)
		}
		if err := client.CopyGraphWithProgress(ctx, srcRepo, targetRepo, desc); err != nil {
			return fmt.Errorf("push: %v", err)
		}
		if tag, ok := targetRef.(reference.Tagged); ok {
			if err := targetRepo.Tag(ctx, desc, tag.Tag()); err != nil {
				return fmt.Errorf("tag: %v", err)
			}
			fmt.Printf("Tag: %s\n", targetRef.String())
		}
		fmt.Printf("Digest: %s\n", fmt.Sprintf("%s@%s", targetRefNamed.Name(), desc.Digest.String()))
	} else {
		tags, err := registry.Tags(ctx, srcRepo)
		if err != nil {
			return fmt.Errorf("get tags from archive: %v", err)
		}

		eg, egCtx := errgroup.WithContext(ctx)
		eg.SetLimit(runtime.NumCPU())
		tagMap := map[string]digest.Digest{}
		var tmm sync.Mutex
		for _, t := range tags {
			t := t
			eg.Go(func() error {
				desc, err := srcRepo.Resolve(egCtx, t)
				if err != nil {
					return fmt.Errorf("resolve archive tag: %v", err)
				}
				if err := client.CopyGraphWithProgress(egCtx, srcRepo, targetRepo, desc); err != nil {
					return fmt.Errorf("push: %v", err)
				}
				if err := targetRepo.Tag(egCtx, desc, t); err != nil {
					return fmt.Errorf("tag: %v", err)
				}
				tmm.Lock()
				defer tmm.Unlock()
				tagMap[t] = desc.Digest
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf("push tags: %v", err)
		}

		for _, t := range tags {
			fmt.Printf("Successfully pushed %s (%s)\n", fmt.Sprintf("%s:%s", targetRefNamed.Name(), t), tagMap[t].String())
		}
	}
	return nil
}
