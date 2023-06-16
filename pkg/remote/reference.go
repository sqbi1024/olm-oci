package remote

import (
	"context"
	"fmt"

	"github.com/containers/image/v5/docker/reference"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	orasremote "oras.land/oras-go/v2/registry/remote"
)

var errNotTaggedOrDigested = fmt.Errorf("reference is not tagged or digested")

func TagOrDigest(ref reference.Reference) (string, error) {
	switch r := ref.(type) {
	case reference.Digested:
		return r.Digest().String(), nil
	case reference.Tagged:
		return r.Tag(), nil
	}
	return "", errNotTaggedOrDigested
}

func ParseNameAndReference(nameAndReference string) (*orasremote.Repository, reference.Named, error) {
	ref, err := reference.ParseNamed(nameAndReference)
	if err != nil {
		return nil, nil, err
	}

	repo, err := NewRepository(ref.Name())
	if err != nil {
		return nil, nil, err
	}
	return repo, ref, nil
}

func ResolveNameAndReference(ctx context.Context, nameAndReference string) (*orasremote.Repository, reference.Reference, *ocispec.Descriptor, error) {
	repo, ref, err := ParseNameAndReference(nameAndReference)
	if err != nil {
		return nil, nil, nil, err
	}

	tagOrDigest, err := TagOrDigest(ref)
	if err != nil {
		return nil, nil, nil, err
	}

	desc, err := repo.Resolve(ctx, tagOrDigest)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to resolve %s: %v", nameAndReference, err)
	}
	return repo, ref, &desc, nil
}
