package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/joelanford/olm-oci/cmd/olmoci/internal/cli"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	c := cobra.Command{
		Use:   "olmoci",
		Short: "Operate on OLM OCI artifacts",
	}
	c.AddCommand(
		cli.NewInspectCommand(),
		cli.NewPushCommand(),
		cli.NewSystemCommand(),
	)

	if err := c.ExecuteContext(ctx); err != nil {
		log.Fatal(err)
	}
}
