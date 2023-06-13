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

func NewPushPackageCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "package <packageDir> <target>",
		Short: "Push an OLM OCI package artifact to a registry.",
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			packageDir := args[0]
			targetRef := args[1]

			if err := runPushPackage(cmd.Context(), packageDir, targetRef); err != nil {
				log.Fatal(err)
			}
		},
	}
}

func runPushPackage(ctx context.Context, packageDir, targetRef string) error {
	repo, ref, err := util.ParseNameAndReference(targetRef)
	if err != nil {
		return fmt.Errorf("parse target reference: %v", err)
	}

	p, err := pkg.LoadPackage(packageDir)
	if err != nil {
		return fmt.Errorf("load package: %v", err)
	}

	desc, err := client.Push(ctx, p, repo)
	if err != nil {
		return fmt.Errorf("push package: %v", err)
	}

	if err := repo.Tag(ctx, *desc, ref.String()); err != nil {
		return fmt.Errorf("tag package: %v", err)
	}
	return nil
}
