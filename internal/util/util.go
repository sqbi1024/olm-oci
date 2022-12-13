package util

import (
	"context"
	"fmt"

	"github.com/docker/distribution/reference"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"
)

func TypeForDescriptor(d ocispec.Descriptor) string {
	if d.ArtifactType != "" {
		return d.ArtifactType
	}
	return d.MediaType
}

func TagOrDigest(ref reference.Reference) (string, error) {
	switch r := ref.(type) {
	case reference.Digested:
		return r.Digest().String(), nil
	case reference.Tagged:
		return r.Tag(), nil
	}
	return "", fmt.Errorf("reference is not tagged or digested")
}

func ResolveNameAndReference(ctx context.Context, nameAndReference string) (*remote.Repository, reference.Reference, *ocispec.Descriptor, error) {
	ref, err := reference.ParseNormalizedNamed(nameAndReference)
	if err != nil {
		return nil, nil, nil, err
	}

	repo, err := remote.NewRepository(ref.Name())
	if err != nil {
		return nil, nil, nil, err
	}

	tagOrDigest, err := TagOrDigest(ref)
	if err != nil {
		return nil, nil, nil, err
	}

	desc, err := repo.Resolve(ctx, tagOrDigest)
	if err != nil {
		return nil, nil, nil, err
	}
	return repo, ref, &desc, nil
}
