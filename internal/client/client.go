package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	orasremote "oras.land/oras-go/v2/registry/remote"

	"github.com/joelanford/olm-oci/internal/json"
	"github.com/joelanford/olm-oci/internal/remote"
)

var DefaultClient = NewClient()

type Client struct {
	repos     map[string]*orasremote.Repository
	repoMutex sync.Mutex
}

type Index interface {
	ArtifactType() string
	Annotations() map[string]string
	SubIndices() []Index
	Blobs() []Blob
}

type Blob interface {
	MediaType() string
	Data() (io.ReadCloser, error)
}

func (c *Client) getRepo(ref string) (*orasremote.Repository, error) {
	c.repoMutex.Lock()
	defer c.repoMutex.Unlock()

	repo, ok := c.repos[ref]
	if !ok {
		var err error
		repo, err = remote.NewRepository(ref)
		if err != nil {
			return nil, err
		}
		c.repos[ref] = repo
	}
	return repo, nil
}

func NewClient() *Client {
	return &Client{
		repos: make(map[string]*orasremote.Repository),
	}
}

func (c *Client) PushToRepo(ctx context.Context, idx Index, repo oras.Target) (*ocispec.Descriptor, error) {
	pushGroup, pushGroupCtx := errgroup.WithContext(ctx)
	pushGroup.SetLimit(8)
	return c.push(pushGroupCtx, pushGroup, idx, repo)
}

func (c *Client) Push(ctx context.Context, idx Index, ref string) (*ocispec.Descriptor, error) {
	repo, err := c.getRepo(ref)
	if err != nil {
		return nil, err
	}

	pushGroup, groupCtx := errgroup.WithContext(ctx)
	pushGroup.SetLimit(8)

	desc, err := c.push(groupCtx, pushGroup, idx, repo)
	if err != nil {
		return nil, err
	}

	if err := pushGroup.Wait(); err != nil {
		return nil, err
	}

	if err := repo.Tag(ctx, *desc, ref); err != nil {
		return nil, err
	}
	return desc, nil
}

type pusher struct {
	pushGroup  *errgroup.Group
	indexGroup *errgroup.Group
	descChan   chan<- ocispec.Descriptor
}

func (c *Client) pushSubIndices(ctx context.Context, p pusher, subIndices []Index, repo oras.Target) {
	for _, si := range subIndices {
		si := si
		p.indexGroup.Go(func() error {
			manifestDesc, err := c.push(ctx, p.pushGroup, si, repo)
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

func (c *Client) pushBlobs(ctx context.Context, p pusher, blobs []Blob, repo oras.Target) {
	for _, blob := range blobs {
		blob := blob
		p.indexGroup.Go(func() error {
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
				errChan <- c.pushIfNotExist(ctx, repo, desc, bytes.NewReader(data))
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

func (c *Client) push(ctx context.Context, pushGroup *errgroup.Group, idx Index, repo oras.Target) (*ocispec.Descriptor, error) {
	eg, egCtx := errgroup.WithContext(ctx)

	numDescs := len(idx.SubIndices()) + len(idx.Blobs())
	descChan := make(chan ocispec.Descriptor, numDescs)
	p := pusher{
		pushGroup:  pushGroup,
		indexGroup: eg,
		descChan:   descChan,
	}
	c.pushSubIndices(egCtx, p, idx.SubIndices(), repo)
	c.pushBlobs(egCtx, p, idx.Blobs(), repo)

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
	//data, _ := json.Marshal(ocispec.Index{
	//	Versioned:   specs.Versioned{SchemaVersion: 2},
	//	MediaType:   ocispec.MediaTypeImageIndex,
	//	Manifests:   descriptors,
	//	Annotations: annotations,
	//})
	//desc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageIndex, data)

	errChan := make(chan error, 1)
	pushGroup.Go(func() error {
		defer close(errChan)
		errChan <- c.pushIfNotExist(ctx, repo, desc, bytes.NewBuffer(data))
		return nil
	})
	if err := <-errChan; err != nil {
		return nil, fmt.Errorf("push index %q with digest %s failed: %w", idx.ArtifactType(), desc.Digest, err)
	}
	return &desc, nil
}

func (c *Client) pushIfNotExist(ctx context.Context, repo oras.Target, desc ocispec.Descriptor, data io.Reader) error {
	exists, err := repo.Exists(ctx, desc)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return repo.Push(ctx, desc, data)
}
