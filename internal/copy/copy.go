package copy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/containers/image/v5/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/reference"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/registry"

	v0 "github.com/joelanford/olm-oci/internal/api/v0"
	"github.com/joelanford/olm-oci/internal/util"
)

func Reference(ctx context.Context, destRefString, srcRefString string) (*ocispec.Descriptor, int64, error) {
	src, _, srcDesc, err := util.ResolveNameAndReference(ctx, srcRefString)
	if err != nil {
		return nil, 0, err
	}
	dst, dstRef, err := util.ParseNameAndReference(destRefString)
	if err != nil {
		return nil, 0, err
	}

	bytesPushed, err := Descriptor(ctx, dst, src, *srcDesc, dstRef)
	if err != nil {
		return nil, bytesPushed, err
	}
	return srcDesc, bytesPushed, nil
}

func Descriptor(ctx context.Context, dest content.Storage, src content.Fetcher, desc ocispec.Descriptor, ref reference.Reference) (int64, error) {
	exists, err := dest.Exists(ctx, desc)
	if err != nil {
		return 0, err
	}
	typ := util.TypeForDescriptor(desc)
	if exists {
		log.Printf("skipped %q with digest %q: already exists", typ, desc.Digest)
		return 0, nil
	}

	rc, err := src.Fetch(ctx, desc)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	push := dest.Push
	if t, ok := ref.(reference.Tagged); ok {
		if refPusher, ok := dest.(registry.ReferencePusher); ok {
			push = func(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error {
				return refPusher.PushReference(ctx, expected, content, t.Tag())
			}
		}
	}

	switch desc.MediaType {
	case manifest.DockerV2Schema2ConfigMediaType,
		manifest.DockerV2Schema2LayerMediaType,
		manifest.DockerV2Schema2ForeignLayerMediaType,
		manifest.DockerV2Schema2ForeignLayerMediaTypeGzip,
		v0.MediaTypeCNCFOperatorFrameworkBundleContentPlainV0TarGZ,
		v0.MediaTypeCNCFOperatorFrameworkChannelEntriesV0YAML,
		v0.MediaTypeCNCFOperatorFrameworkConstraintsV0YAML,
		v0.MediaTypeCNCFOperatorFrameworkPropertiesV0YAML,
		"text/markdown",
		"image/svg+xml":
		if err := push(ctx, desc, rc); err != nil {
			return 0, fmt.Errorf("failed pushing %q with digest %q: %v", typ, desc.Digest, err)
		}
		log.Printf("pushed %q with digest %q", typ, desc.Digest)
		return desc.Size, nil
	}

	blob, err := io.ReadAll(rc)
	if err != nil {
		return 0, err
	}

	var bytesPushed int64
	switch desc.MediaType {
	case ocispec.MediaTypeArtifactManifest:
		var v ocispec.Artifact
		if err := json.Unmarshal(blob, &v); err != nil {
			return bytesPushed, err
		}
		for _, artifactBlob := range v.Blobs {
			size, err := Descriptor(ctx, dest, src, artifactBlob, nil)
			bytesPushed += size
			if err != nil {
				return bytesPushed, err
			}
		}
	case ocispec.MediaTypeImageIndex:
		var v ocispec.Index
		if err := json.Unmarshal(blob, &v); err != nil {
			return bytesPushed, err
		}
		for _, m := range v.Manifests {
			size, err := Descriptor(ctx, dest, src, m, nil)
			bytesPushed += size
			if err != nil {
				return bytesPushed, err
			}
		}
	case ocispec.MediaTypeImageManifest:
		var v ocispec.Manifest
		if err := json.Unmarshal(blob, &v); err != nil {
			return bytesPushed, err
		}
		size, err := Descriptor(ctx, dest, src, v.Config, nil)
		bytesPushed += size
		if err != nil {
			return bytesPushed, err
		}
		for _, layer := range v.Layers {
			size, err := Descriptor(ctx, dest, src, layer, nil)
			bytesPushed += size
			if err != nil {
				return bytesPushed, err
			}
		}
	case manifestlist.MediaTypeManifestList:
		var v manifestlist.ManifestList
		if err := json.Unmarshal(blob, &v); err != nil {
			return bytesPushed, err
		}
		for _, m := range v.Manifests {
			size, err := Descriptor(ctx, dest, src, manifestDescriptorToOCIDescriptor(m), nil)
			bytesPushed += size
			if err != nil {
				return bytesPushed, err
			}
		}
	case manifest.DockerV2Schema2MediaType:
		var v manifest.Schema2
		if err := json.Unmarshal(blob, &v); err != nil {
			return bytesPushed, err
		}
		size, err := Descriptor(ctx, dest, src, schema2DescriptorToOCIDescriptor(v.ConfigDescriptor), nil)
		bytesPushed += size
		if err != nil {
			return bytesPushed, err
		}
		for _, l := range v.LayersDescriptors {
			size, err := Descriptor(ctx, dest, src, schema2DescriptorToOCIDescriptor(l), nil)
			bytesPushed += size
			if err != nil {
				return bytesPushed, err
			}
		}
	default:
		return bytesPushed, fmt.Errorf("unrecognized media type %q", desc.MediaType)
	}

	if err := push(ctx, desc, bytes.NewReader(blob)); err != nil {
		return bytesPushed, fmt.Errorf("failed pushing %q with digest %q: %v", typ, desc.Digest, err)
	}
	log.Printf("pushed %q with digest %q", typ, desc.Digest)
	bytesPushed += desc.Size
	return bytesPushed, nil
}

func manifestDescriptorToOCIDescriptor(d manifestlist.ManifestDescriptor) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType:   d.MediaType,
		Digest:      d.Digest,
		Size:        d.Size,
		URLs:        d.URLs,
		Annotations: d.Annotations,
		Platform: &ocispec.Platform{
			Architecture: d.Platform.Architecture,
			OS:           d.Platform.OS,
			OSVersion:    d.Platform.OSVersion,
			OSFeatures:   d.Platform.OSFeatures,
			Variant:      d.Platform.Variant,
		},
	}
}
func schema2DescriptorToOCIDescriptor(d manifest.Schema2Descriptor) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType: d.MediaType,
		Digest:    d.Digest,
		Size:      d.Size,
		URLs:      d.URLs,
	}
}
