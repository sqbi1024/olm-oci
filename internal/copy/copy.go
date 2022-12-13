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
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	v0 "github.com/joelanford/olm-oci/internal/api/v0"
	"github.com/joelanford/olm-oci/internal/util"
)

func Reference(ctx context.Context, dest oras.Target, srcRefString string) (*ocispec.Descriptor, int64, error) {
	src, _, desc, err := util.ResolveNameAndReference(ctx, srcRefString)
	if err != nil {
		return nil, 0, err
	}
	bytesPushed, err := Descriptor(ctx, src, dest, *desc)
	if err != nil {
		return nil, bytesPushed, err
	}
	return desc, bytesPushed, nil
}

func Descriptor(ctx context.Context, src, dest oras.Target, desc ocispec.Descriptor) (int64, error) {
	exists, err := dest.Exists(ctx, desc)
	if err != nil {
		return 0, err
	}
	typ := util.TypeForDescriptor(desc)
	if exists {
		log.Printf("skipping %q with digest %q: already exists in repo", typ, desc.Digest)
		return 0, nil
	}

	rc, err := src.Fetch(ctx, desc)
	if err != nil {
		return 0, err
	}
	defer rc.Close()

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
			size, err := Descriptor(ctx, src, dest, artifactBlob)
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
			size, err := Descriptor(ctx, src, dest, m)
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
		size, err := Descriptor(ctx, src, dest, v.Config)
		bytesPushed += size
		if err != nil {
			return bytesPushed, err
		}
		for _, layer := range v.Layers {
			size, err := Descriptor(ctx, src, dest, layer)
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
			size, err := Descriptor(ctx, src, dest, manifestDescriptorToOCIDescriptor(m))
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
		size, err := Descriptor(ctx, src, dest, schema2DescriptorToOCIDescriptor(v.ConfigDescriptor))
		bytesPushed += size
		if err != nil {
			return bytesPushed, err
		}
		for _, l := range v.LayersDescriptors {
			size, err := Descriptor(ctx, src, dest, schema2DescriptorToOCIDescriptor(l))
			bytesPushed += size
			if err != nil {
				return bytesPushed, err
			}
		}
	case ocispec.MediaTypeImageConfig,
		ocispec.MediaTypeImageLayer,
		ocispec.MediaTypeImageLayerGzip,
		ocispec.MediaTypeImageLayerZstd,
		ocispec.MediaTypeImageLayerNonDistributable,
		ocispec.MediaTypeImageLayerNonDistributableGzip,
		ocispec.MediaTypeImageLayerNonDistributableZstd,
		manifest.DockerV2Schema2ConfigMediaType,
		manifest.DockerV2Schema2LayerMediaType,
		manifest.DockerV2Schema2ForeignLayerMediaType,
		manifest.DockerV2Schema2ForeignLayerMediaTypeGzip,
		v0.MediaTypeCNCFOperatorFrameworkBundleContentPlainV0TarGZ,
		v0.MediaTypeCNCFOperatorFrameworkChannelEntriesV0YAML,
		v0.MediaTypeCNCFOperatorFrameworkConstraintsV0YAML,
		v0.MediaTypeCNCFOperatorFrameworkPropertiesV0YAML,
		"text/markdown",
		"image/svg+xml":
	default:
		return bytesPushed, fmt.Errorf("unrecognized media type %q", desc.MediaType)
	}
	log.Printf("pushing %q with digest %q", typ, desc.Digest)
	if err := dest.Push(ctx, desc, bytes.NewReader(blob)); err != nil {
		return bytesPushed, fmt.Errorf("failed pushing %q with digest %q: %v", typ, desc.Digest, err)
	}
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
