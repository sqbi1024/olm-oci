package cli

import (
	"context"
	"fmt"
	"log"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/internal/client"
	"github.com/joelanford/olm-oci/internal/pkg"
	"github.com/joelanford/olm-oci/internal/util"
)

func NewPushBundleCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "bundle <bundleDir> <target>",
		Short: "Push an OLM OCI bundle artifact to a registry.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			bundleDir := args[0]
			targetRef := args[1]

			if err := runPushBundle(cmd.Context(), bundleDir, targetRef); err != nil {
				log.Fatal(err)
			}
		},
	}
}

func runPushBundle(ctx context.Context, bundleDir, targetRef string) error {
	repo, ref, err := util.ParseNameAndReference(targetRef)
	if err != nil {
		return fmt.Errorf("parse target reference: %v", err)
	}
	b, err := pkg.LoadBundle(bundleDir)
	if err != nil {
		return fmt.Errorf("load bundle: %v", err)
	}
	desc, err := client.Push(ctx, b, repo)
	if err != nil {
		return fmt.Errorf("push bundle: %v", err)
	}
	if err := repo.Tag(ctx, *desc, ref.String()); err != nil {
		return fmt.Errorf("tag bundle: %v", err)
	}
	return nil
}
