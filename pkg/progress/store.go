package progress

import (
	"context"
	"io"

	"github.com/docker/docker/pkg/progress"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
)

func NewStore(base content.ReadOnlyStorage, out progress.Output) content.ReadOnlyStorage {
	return &Store{
		base: base,
		out:  out,
	}
}

type Store struct {
	base content.ReadOnlyStorage
	out  progress.Output
}

func (s *Store) Exists(ctx context.Context, desc ocispec.Descriptor) (bool, error) {
	return s.base.Exists(ctx, desc)
}

func (s *Store) Fetch(ctx context.Context, desc ocispec.Descriptor) (io.ReadCloser, error) {
	rc, err := s.base.Fetch(ctx, desc)
	if err != nil {
		return nil, err
	}

	return progress.NewProgressReader(rc, s.out, desc.Size, IDForDesc(desc), "Pushing "), nil
}

func IDForDesc(desc ocispec.Descriptor) string {
	return desc.Digest.String()[7:19]
}
