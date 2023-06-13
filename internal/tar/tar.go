package tar

import (
	"archive/tar"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

func WriteFS(fsys fs.FS, w io.Writer) (returnErr error) {
	tw := tar.NewWriter(w)
	defer func() {
		if err := tw.Close(); err != nil && returnErr == nil {
			returnErr = err
			return
		}
	}()

	return fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		path = filepath.ToSlash(path)

		// Generate header
		mode := info.Mode()
		if mode&os.ModeSymlink != 0 {
			return fmt.Errorf("symlinks are not supported: %s", path)
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		header.Name = path
		header.Uid = 0
		header.Gid = 0
		header.Uname = ""
		header.Gname = ""

		// Write file
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("tar: %w", err)
		}
		if !mode.IsRegular() {
			return nil
		}
		fp, err := fsys.Open(path)
		if err != nil {
			return err
		}
		defer func() {
			closeErr := fp.Close()
			if returnErr == nil {
				returnErr = closeErr
			}
		}()

		if _, err := io.Copy(tw, fp); err != nil {
			return fmt.Errorf("failed to copy to %s: %w", path, err)
		}

		return nil
	})
}
