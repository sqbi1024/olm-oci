package fetch

import (
	"context"
	"fmt"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"oras.land/oras-go/v2/content"

	pkg "github.com/joelanford/olm-oci/api/v1"
	"github.com/joelanford/olm-oci/pkg/inspect"
)

func IgnoreMediaTypes(mediaTypes ...string) func(context.Context, content.Fetcher, v1.Descriptor) ([]ocispec.Descriptor, error) {
	skipSet := sets.New[string](mediaTypes...)
	return func(ctx context.Context, fetcher content.Fetcher, desc v1.Descriptor) ([]v1.Descriptor, error) {
		successors, err := content.Successors(ctx, fetcher, desc)
		if err != nil {
			return nil, fmt.Errorf("get successors: %v", err)
		}
		out := successors[:0]
		for _, succ := range successors {
			if skipSet.Has(succ.MediaType) {
				continue
			}
			out = append(out, succ)
		}
		return out, nil
	}
}

func FetchArtifact(ctx context.Context, src content.Fetcher, desc ocispec.Descriptor) (ocispec.Artifact, error) {
	if desc.MediaType != ocispec.MediaTypeArtifactManifest {
		return ocispec.Artifact{}, fmt.Errorf("expected artifact manifest, got %q", desc.MediaType)
	}
	rc, err := src.Fetch(ctx, desc)
	if err != nil {
		return ocispec.Artifact{}, fmt.Errorf("fetch artifact manifest: %v", err)
	}
	defer rc.Close()
	return inspect.DecodeArtifact(rc)
}

func FetchPackage(ctx context.Context, src content.Fetcher, pkgArtifact ocispec.Artifact, skipMediaTypes ...string) (*pkg.Package, error) {
	if pkgArtifact.ArtifactType != pkg.MediaTypePackage {
		return nil, fmt.Errorf("expected artifact type %q, got %q", pkg.MediaTypePackage, pkgArtifact.ArtifactType)
	}
	skips := sets.New[string](skipMediaTypes...)

	var p pkg.Package
	for _, b := range pkgArtifact.Blobs {
		if skips.Has(b.MediaType) {
			continue
		}
		if err := func() error {
			br, err := src.Fetch(ctx, b)
			if err != nil {
				return fmt.Errorf("fetch blob: %v", err)
			}
			defer br.Close()

			if b.MediaType == ocispec.MediaTypeArtifactManifest {
				blobArt, err := inspect.DecodeArtifact(br)
				if err != nil {
					return fmt.Errorf("decode artifact manifest: %v", err)
				}
				switch blobArt.ArtifactType {
				case pkg.MediaTypeChannel:
					ch, err := FetchChannel(ctx, src, blobArt, skipMediaTypes...)
					if err != nil {
						return fmt.Errorf("fetch channel: %v", err)
					}
					p.Channels = append(p.Channels, *ch)
				default:
					return fmt.Errorf("expected artifact type %q, got %q", pkg.MediaTypeChannel, blobArt.ArtifactType)
				}
				return nil
			}

			switch b.MediaType {
			case pkg.MediaTypePackageMetadata:
				p.Metadata, err = inspect.DecodePackageMetadata(br)
			case pkg.MediaTypeUpgradeEdges:
				p.UpgradeEdges, err = inspect.DecodeUpgradeEdges(br)
			case pkg.MediaTypeProperties:
				p.Properties, err = inspect.DecodeProperties(br)
			case "image/png", "image/svg+xml":
				var icon pkg.Icon
				icon, err = inspect.DecodeIcon(b.MediaType, br)
				p.Icon = &icon
			case "text/markdown":
				p.Description, err = inspect.DecodeDescription(br)
			default:
				return fmt.Errorf("unsupported package blob media type %q", b.MediaType)
			}
			return err
		}(); err != nil {
			return nil, err
		}
	}

	return &p, nil
}

func FetchChannel(ctx context.Context, src content.Fetcher, chArt ocispec.Artifact, skipMediaTypes ...string) (*pkg.Channel, error) {
	if chArt.ArtifactType != pkg.MediaTypeChannel {
		return nil, fmt.Errorf("expected artifact type %q, got %q", pkg.MediaTypeChannel, chArt.ArtifactType)
	}
	skips := sets.New[string](skipMediaTypes...)

	var ch pkg.Channel
	for _, b := range chArt.Blobs {
		if skips.Has(b.MediaType) {
			continue
		}
		if err := func() error {
			br, err := src.Fetch(ctx, b)
			if err != nil {
				return fmt.Errorf("fetch blob: %v", err)
			}
			defer br.Close()

			if b.MediaType == ocispec.MediaTypeArtifactManifest {
				blobArt, err := inspect.DecodeArtifact(br)
				if err != nil {
					return fmt.Errorf("decode artifact manifest: %v", err)
				}
				switch blobArt.ArtifactType {
				case pkg.MediaTypeBundle:
					bundle, err := FetchBundle(ctx, src, blobArt, skipMediaTypes...)
					if err != nil {
						return fmt.Errorf("fetch channel: %v", err)
					}
					bundle.Digest = b.Digest
					ch.Bundles = append(ch.Bundles, *bundle)
				default:
					return fmt.Errorf("expected artifact type %q, got %q", pkg.MediaTypeBundle, blobArt.ArtifactType)
				}
				return nil
			}

			switch b.MediaType {
			case pkg.MediaTypeChannelMetadata:
				ch.Metadata, err = inspect.DecodeChannelMetadata(br)
			case pkg.MediaTypeProperties:
				ch.Properties, err = inspect.DecodeProperties(br)
			case ocispec.MediaTypeArtifactManifest:
				return fmt.Errorf("unsupported channel blob artifact type %q", b.ArtifactType)
			default:
				return fmt.Errorf("unsupported channel blob media type %q", b.MediaType)
			}
			return err
		}(); err != nil {
			return nil, err
		}
	}
	return &ch, nil
}

func FetchBundle(ctx context.Context, src content.Fetcher, bArt ocispec.Artifact, skipMediaTypes ...string) (*pkg.Bundle, error) {
	if bArt.ArtifactType != pkg.MediaTypeBundle {
		return nil, fmt.Errorf("expected artifact type %q, got %q", pkg.MediaTypeBundle, bArt.ArtifactType)
	}
	skips := sets.New[string](skipMediaTypes...)

	var bundle pkg.Bundle
	for _, b := range bArt.Blobs {
		if skips.Has(b.MediaType) {
			continue
		}
		if err := func() error {
			br, err := src.Fetch(ctx, b)
			if err != nil {
				return fmt.Errorf("fetch blob: %v", err)
			}
			defer br.Close()

			switch b.MediaType {
			case pkg.MediaTypeBundleMetadata:
				bundle.Metadata, err = inspect.DecodeBundleMetadata(br)
			case pkg.MediaTypeProperties:
				bundle.Properties, err = inspect.DecodeProperties(br)
			case pkg.MediaTypeConstraints:
				bundle.Constraints, err = inspect.DecodeConstraints(br)
			case pkg.MediaTypeRelatedImages:
				bundle.RelatedImages, err = inspect.DecodeRelatedImages(br)
			case pkg.MediaTypeBundleContent:
				bundle.Content, err = inspect.DecodeBundleContent(br)
			default:
				return fmt.Errorf("unsupported bundle blob type %q", b.MediaType)
			}
			return err
		}(); err != nil {
			return nil, err
		}
	}
	return &bundle, nil
}
