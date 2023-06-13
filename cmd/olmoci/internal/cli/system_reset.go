package cli

import (
	"log"
	"os"
	"path/filepath"

	"github.com/adrg/xdg"
	"github.com/spf13/cobra"
)

func NewSystemResetCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "reset",
		Short: "Clear all cached data",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			cacheDir := filepath.Join(xdg.CacheHome, "olm-oci")
			if err := os.RemoveAll(cacheDir); err != nil {
				log.Fatal(err)
			}
		},
	}
}
