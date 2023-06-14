package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"

	"github.com/docker/docker/pkg/jsonmessage"
	dockerprogress "github.com/docker/docker/pkg/progress"
	"github.com/docker/docker/pkg/streamformatter"
	"github.com/go-logr/logr"
	"github.com/mattn/go-isatty"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"oras.land/oras-go/v2"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/content/memory"
	"oras.land/oras-go/v2/errdef"

	"github.com/joelanford/olm-oci/pkg/progress"
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

type Client struct {
	Target oras.Target
	Log    logr.Logger
}

func Push(ctx context.Context, artifact Artifact, target oras.Target) (ocispec.Descriptor, error) {
	store := memory.New()
	desc, err := push(ctx, artifact, store)
	if err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("stage artifact graph locally: %v", err)
	}

	if err := CopyGraphWithProgress(ctx, store, target, desc); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("push artifact graph: %v", err)
	}
	return desc, nil
}

func pushSubIndices(ctx context.Context, eg *errgroup.Group, descChan chan<- ocispec.Descriptor, subIndices []Artifact, store *memory.Store) {
	for _, si := range subIndices {
		si := si
		eg.Go(func() error {
			manifestDesc, err := push(ctx, si, store)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case descChan <- manifestDesc:
			}
			return nil
		})
	}
}

func pushBlobs(ctx context.Context, eg *errgroup.Group, descChan chan<- ocispec.Descriptor, blobs []Blob, store *memory.Store) {
	for _, blob := range blobs {
		blob := blob
		eg.Go(func() error {
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
			if err := pushIfNotExist(ctx, store, desc, bytes.NewReader(data)); err != nil {
				return fmt.Errorf("push blob %q with digest %s failed: %w", desc.MediaType, desc.Digest, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case descChan <- desc:
				return nil
			}
		})
	}
}

func CopyGraphWithProgress(ctx context.Context, src oras.Target, dst oras.Target, desc ocispec.Descriptor) error {
	pr, pw := io.Pipe()
	fd := os.Stdout.Fd()
	isTTY := isatty.IsTerminal(fd)
	out := streamformatter.NewJSONProgressOutput(pw, !isTTY)
	ps := progress.NewStore(src, out)
	errChan := make(chan error, 1)
	go func() {
		errChan <- jsonmessage.DisplayJSONMessagesStream(pr, os.Stdout, fd, isTTY, nil)
	}()
	opts := oras.CopyGraphOptions{
		Concurrency: runtime.NumCPU(),
		OnCopySkipped: func(ctx context.Context, desc ocispec.Descriptor) error {
			return out.WriteProgress(dockerprogress.Progress{
				ID:     progress.IDForDesc(desc),
				Action: "Artifact is up to date",
			})
		},
		PostCopy: func(_ context.Context, desc ocispec.Descriptor) error {
			return out.WriteProgress(dockerprogress.Progress{
				ID:      progress.IDForDesc(desc),
				Action:  "Complete",
				Current: desc.Size,
				Total:   desc.Size,
			})
		},
	}
	if err := oras.CopyGraph(ctx, ps, dst, desc, opts); err != nil {
		return fmt.Errorf("copy artifact graph: %v", err)
	}
	if err := pw.Close(); err != nil {
		return fmt.Errorf("close progress writer: %v", err)
	}
	if err := <-errChan; err != nil {
		return fmt.Errorf("display progress: %v", err)
	}
	return nil
}

func push(ctx context.Context, artifact Artifact, store *memory.Store) (ocispec.Descriptor, error) {
	eg, egCtx := errgroup.WithContext(ctx)
	numDescs := len(artifact.SubIndices()) + len(artifact.Blobs())
	descChan := make(chan ocispec.Descriptor, numDescs)

	pushSubIndices(egCtx, eg, descChan, artifact.SubIndices(), store)
	pushBlobs(egCtx, eg, descChan, artifact.Blobs(), store)

	if err := eg.Wait(); err != nil {
		return ocispec.Descriptor{}, err
	}
	close(descChan)

	descriptors := make([]ocispec.Descriptor, 0, numDescs)
	for desc := range descChan {
		descriptors = append(descriptors, desc)
	}
	sort.Slice(descriptors, func(i, j int) bool {
		return descriptors[i].Digest.String() < descriptors[j].Digest.String()
	})

	data, _ := json.Marshal(ocispec.Artifact{
		MediaType:    ocispec.MediaTypeArtifactManifest,
		ArtifactType: artifact.ArtifactType(),
		Blobs:        descriptors,
		Annotations:  artifact.Annotations(),
	})
	desc := content.NewDescriptorFromBytes(ocispec.MediaTypeArtifactManifest, data)

	//annotations := artifact.Annotations()
	//annotations[pkg.AnnotationKeyArtifactType] = artifact.ArtifactType()
	//data, _ := json.Marshal(ocispec.Artifact{
	//	Versioned:   specs.Versioned{SchemaVersion: 2},
	//	MediaType:   ocispec.MediaTypeImageIndex,
	//	Manifests:   descriptors,
	//	Annotations: annotations,
	//})
	//desc := content.NewDescriptorFromBytes(ocispec.MediaTypeImageIndex, data)

	if err := pushIfNotExist(ctx, store, desc, bytes.NewBuffer(data)); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("push artifact %q with digest %s failed: %w", artifact.ArtifactType(), desc.Digest, err)
	}
	return desc, nil
}

func pushIfNotExist(ctx context.Context, store *memory.Store, desc ocispec.Descriptor, r io.Reader) error {
	if err := store.Push(ctx, desc, r); err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
		return err
	}
	return nil
}
