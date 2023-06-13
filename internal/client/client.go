package client

import (
	"bytes"
	"context"
	"fmt"
	"io"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"

	"github.com/joelanford/olm-oci/internal/json"
)

type Artifact interface {
	ArtifactType() string
	Annotations() map[string]string
	SubIndices() []Artifact
	Blobs() []Blob
}

type Blob interface {
	MediaType() string
	Data() (io.ReadCloser, error)
}

func Push(ctx context.Context, idx Artifact, repo oras.Target) (*ocispec.Descriptor, error) {
	pushGroup, groupCtx := errgroup.WithContext(ctx)
	pushGroup.SetLimit(8)

	desc, err := push(groupCtx, pushGroup, idx, repo)
	if err != nil {
		return nil, err
	}

	if err := pushGroup.Wait(); err != nil {
		return nil, err
	}
	return desc, nil
}

type pusher struct {
	pushGroup     *errgroup.Group
	artifactGroup *errgroup.Group
	descChan      chan<- ocispec.Descriptor
}

func pushSubIndices(ctx context.Context, p pusher, subIndices []Artifact, repo oras.Target) {
	for _, si := range subIndices {
		si := si
		p.artifactGroup.Go(func() error {
			manifestDesc, err := push(ctx, p.pushGroup, si, repo)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.descChan <- *manifestDesc:
			}
			return nil
		})
	}
}

func pushBlobs(ctx context.Context, p pusher, blobs []Blob, repo oras.Target) {
	for _, blob := range blobs {
		blob := blob
		p.artifactGroup.Go(func() error {
			rc, err := blob.Data()
			if err != nil {
				return err
			}
			defer rc.Close()
			data, err := io.ReadAll(rc)
			if err != nil {
				return err
			}

			desc := content.NewDescriptorFromBytes(blob.MediaType(), data)

			errChan := make(chan error)
			p.pushGroup.Go(func() error {
				defer close(errChan)
				errChan <- pushIfNotExist(ctx, repo, desc, bytes.NewReader(data))
				return nil
			})
			if err := <-errChan; err != nil {
				return fmt.Errorf("push blob %q with digest %s failed: %w", desc.MediaType, desc.Digest, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case p.descChan <- desc:
				return nil
			}
		})
	}
}

func push(ctx context.Context, pushGroup *errgroup.Group, idx Artifact, repo oras.Target) (*ocispec.Descriptor, error) {
	eg, egCtx := errgroup.WithContext(ctx)

	numDescs := len(idx.SubIndices()) + len(idx.Blobs())
	descChan := make(chan ocispec.Descriptor, numDescs)
	p := pusher{
		pushGroup:     pushGroup,
		artifactGroup: eg,
		descChan:      descChan,
	}
	pushSubIndices(egCtx, p, idx.SubIndices(), repo)
	pushBlobs(egCtx, p, idx.Blobs(), repo)

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	close(descChan)

	descriptors := make([]ocispec.Descriptor, 0, numDescs)
	for desc := range descChan {
		descriptors = append(descriptors, desc)
	}

	data, _ := json.Marshal(ocispec.Artifact{
		MediaType:    ocispec.MediaTypeArtifactManifest,
		ArtifactType: idx.ArtifactType(),
		Blobs:        descriptors,
		Annotations:  idx.Annotations(),
	})
	desc := content.NewDescriptorFromBytes(ocispec.MediaTypeArtifactManifest, data)

	//annotations := idx.Annotations()
	//annotations[pkg.AnnotationKeyArtifactType] = idx.ArtifactType()
	//data, _ := json.Marshal(ocispec.Artifact{
	//	Versioned:   specs.Versioned{SchemaVersion: 2},
	//	MediaType:   ocispec.MediaTypeImageIndex,
	//	Manifests:   descriptors,
	//	Annotations: annotations,
	//})
	//desc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageIndex, data)

	errChan := make(chan error, 1)
	pushGroup.Go(func() error {
		defer close(errChan)
		errChan <- pushIfNotExist(ctx, repo, desc, bytes.NewBuffer(data))
		return nil
	})
	if err := <-errChan; err != nil {
		return nil, fmt.Errorf("push artifact %q with digest %s failed: %w", idx.ArtifactType(), desc.Digest, err)
	}
	return &desc, nil
}

func pushIfNotExist(ctx context.Context, repo oras.Target, desc ocispec.Descriptor, data io.Reader) error {
	exists, err := repo.Exists(ctx, desc)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return repo.Push(ctx, desc, data)
}
