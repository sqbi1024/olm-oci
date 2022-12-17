package push

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/docker/distribution/reference"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"gopkg.in/yaml.v2"
	"oras.land/oras-go/v2"

	v0 "github.com/joelanford/olm-oci/internal/api/v0"
	"github.com/joelanford/olm-oci/internal/copy"
	"github.com/joelanford/olm-oci/internal/util"
)

func Package(ctx context.Context, target oras.Target, packageDir string) (*ocispec.Descriptor, int64, error) {
	var bytesPushed int64

	channelsDirPath := filepath.Join(packageDir, "channels")
	channelsDirEntries, err := os.ReadDir(channelsDirPath)
	if err != nil {
		return nil, bytesPushed, err
	}
	descs := make([]ocispec.Descriptor, 0, len(channelsDirEntries))
	for _, channelDirEntry := range channelsDirEntries {
		channel, err := channelDirEntry.Info()
		if err != nil {
			return nil, bytesPushed, err
		}
		path := filepath.Join(channelsDirPath, channel.Name())
		if channel.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return nil, bytesPushed, err
			}
			path = filepath.Join(channelsDirPath, link)
			channel, err = os.Stat(path)
			if err != nil {
				return nil, bytesPushed, err
			}
		}
		if !channel.IsDir() {
			return nil, bytesPushed, fmt.Errorf("encountered non-directory %q: expected operatorframework channel directory", path)
		}
		desc, channelBytesPushed, err := Channel(ctx, target, path, channel.Name())
		bytesPushed += channelBytesPushed
		if err != nil {
			return nil, bytesPushed, err
		}
		descs = append(descs, *desc)
	}

	readmeData, err := os.ReadFile(filepath.Join(packageDir, "README.md"))
	if err != nil {
		return nil, bytesPushed, err
	}
	readmeDesc := ocispec.Descriptor{
		MediaType: "text/markdown",
		Digest:    digest.FromBytes(readmeData),
		Size:      int64(len(readmeData)),
	}
	readmeBytesPushed, err := pushIfNotExist(ctx, target, readmeDesc, io.NopCloser(bytes.NewReader(readmeData)), nil)
	bytesPushed += readmeBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}

	iconData, err := os.ReadFile(filepath.Join(packageDir, "icon.svg"))
	if err != nil {
		return nil, bytesPushed, err
	}
	iconDesc := ocispec.Descriptor{
		MediaType: "image/svg+xml",
		Digest:    digest.FromBytes(iconData),
		Size:      int64(len(iconData)),
	}
	iconBytesPushed, err := pushIfNotExist(ctx, target, iconDesc, io.NopCloser(bytes.NewReader(iconData)), nil)
	bytesPushed += iconBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}

	properties, err := os.ReadFile(filepath.Join(packageDir, "properties.yaml"))
	if err != nil {
		return nil, bytesPushed, err
	}
	propertiesDesc := ocispec.Descriptor{
		MediaType: v0.MediaTypeCNCFOperatorFrameworkPropertiesV0YAML,
		Digest:    digest.FromBytes(properties),
		Size:      int64(len(properties)),
	}
	propertiesBytesPushed, err := pushIfNotExist(ctx, target, propertiesDesc, io.NopCloser(bytes.NewReader(properties)), nil)
	bytesPushed += propertiesBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}

	//artifact, _ := json.Marshal(ocispec.Artifact{
	//	MediaType:    ocispec.MediaTypeArtifactManifest,
	//	ArtifactType: v0.MediaTypeCNCFOperatorFrameworkPackageV0,
	//	Blobs:        append([]ocispec.Descriptor{entriesDesc, propertiesDesc}, descs...),
	//	Annotations:  map[string]string{"tag": tag},
	//})
	//artifactDesc := ocispec.Descriptor{
	//	MediaType:    ocispec.MediaTypeArtifactManifest,
	//	ArtifactType: v0.MediaTypeCNCFOperatorFrameworkPackageV0,
	//	Digest:       digest.FromBytes(artifact),
	//	Size:         int64(len(artifact)),
	//	Annotations:  map[string]string{"tag": tag},
	//}

	artifact, _ := json.Marshal(ocispec.Index{
		Versioned:   specs.Versioned{SchemaVersion: 2},
		MediaType:   ocispec.MediaTypeImageIndex,
		Manifests:   append([]ocispec.Descriptor{readmeDesc, iconDesc, propertiesDesc}, descs...),
		Annotations: map[string]string{"artifactType": v0.MediaTypeCNCFOperatorFrameworkPackageV0},
	})
	artifactDesc := ocispec.Descriptor{
		MediaType:    ocispec.MediaTypeImageIndex,
		ArtifactType: v0.MediaTypeCNCFOperatorFrameworkPackageV0,
		Digest:       digest.FromBytes(artifact),
		Size:         int64(len(artifact)),
		Annotations:  map[string]string{"artifactType": v0.MediaTypeCNCFOperatorFrameworkPackageV0},
	}
	artifactBytesPushed, err := pushIfNotExist(ctx, target, artifactDesc, io.NopCloser(bytes.NewReader(artifact)), tag("package"))
	bytesPushed += artifactBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}
	return &artifactDesc, bytesPushed, nil
}

func Channel(ctx context.Context, target oras.Target, channelDir, channelName string) (*ocispec.Descriptor, int64, error) {
	var bytesPushed int64
	bundlesDirPath := filepath.Join(channelDir, "bundles")
	bundlesDirEntries, err := os.ReadDir(bundlesDirPath)
	if err != nil {
		return nil, bytesPushed, err
	}
	descs := make([]ocispec.Descriptor, 0, len(bundlesDirEntries))
	for _, bundleDirEntry := range bundlesDirEntries {
		bundle, err := bundleDirEntry.Info()
		if err != nil {
			return nil, bytesPushed, err
		}
		path := filepath.Join(bundlesDirPath, bundle.Name())
		if bundle.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return nil, bytesPushed, err
			}
			path = filepath.Join(bundlesDirPath, link)
			bundle, err = os.Stat(path)
			if err != nil {
				return nil, bytesPushed, err
			}
		}
		if !bundle.IsDir() {
			return nil, bytesPushed, fmt.Errorf("encountered non-directory %q: expected operatorframework bundle directory", path)
		}
		desc, bundleBytesPushed, err := Bundle(ctx, target, path, bundle.Name())
		bytesPushed += bundleBytesPushed
		if err != nil {
			return nil, bytesPushed, err
		}
		descs = append(descs, *desc)
	}

	entriesYAML, err := os.ReadFile(filepath.Join(channelDir, "entries.yaml"))
	if err != nil {
		return nil, bytesPushed, err
	}
	entriesDesc := ocispec.Descriptor{
		MediaType: v0.MediaTypeCNCFOperatorFrameworkChannelEntriesV0YAML,
		Digest:    digest.FromBytes(entriesYAML),
		Size:      int64(len(entriesYAML)),
	}
	entriesBytesPushed, err := pushIfNotExist(ctx, target, entriesDesc, io.NopCloser(bytes.NewReader(entriesYAML)), nil)
	bytesPushed += entriesBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}

	properties, err := os.ReadFile(filepath.Join(channelDir, "properties.yaml"))
	if err != nil {
		return nil, bytesPushed, err
	}
	propertiesDesc := ocispec.Descriptor{
		MediaType: v0.MediaTypeCNCFOperatorFrameworkPropertiesV0YAML,
		Digest:    digest.FromBytes(properties),
		Size:      int64(len(properties)),
	}

	propertyBytesPushed, err := pushIfNotExist(ctx, target, propertiesDesc, io.NopCloser(bytes.NewReader(properties)), nil)
	bytesPushed += propertyBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}

	//artifact, _ := json.Marshal(ocispec.Artifact{
	//	MediaType:    ocispec.MediaTypeArtifactManifest,
	//	ArtifactType: v0.MediaTypeCNCFOperatorFrameworkChannelV0,
	//	Blobs:        append([]ocispec.Descriptor{entriesDesc, propertiesDesc}, descs...),
	//	Annotations:  map[string]string{"tag": tag},
	//})
	//artifactDesc := ocispec.Descriptor{
	//	MediaType:    ocispec.MediaTypeArtifactManifest,
	//	ArtifactType: v0.MediaTypeCNCFOperatorFrameworkChannelV0,
	//	Digest:       digest.FromBytes(artifact),
	//	Size:         int64(len(artifact)),
	//	Annotations:  map[string]string{"tag": tag},
	//}

	artifact, _ := json.Marshal(ocispec.Index{
		Versioned:   specs.Versioned{SchemaVersion: 2},
		MediaType:   ocispec.MediaTypeImageIndex,
		Manifests:   append([]ocispec.Descriptor{entriesDesc, propertiesDesc}, descs...),
		Annotations: map[string]string{"artifactType": v0.MediaTypeCNCFOperatorFrameworkChannelV0},
	})
	artifactDesc := ocispec.Descriptor{
		MediaType:    ocispec.MediaTypeImageIndex,
		ArtifactType: v0.MediaTypeCNCFOperatorFrameworkChannelV0,
		Digest:       digest.FromBytes(artifact),
		Size:         int64(len(artifact)),
		Annotations:  map[string]string{"artifactType": v0.MediaTypeCNCFOperatorFrameworkChannelV0},
	}
	artifactBytesPushed, err := pushIfNotExist(ctx, target, artifactDesc, io.NopCloser(bytes.NewReader(artifact)), tag(fmt.Sprintf("channel.%s", channelName)))
	bytesPushed += artifactBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}
	return &artifactDesc, bytesPushed, nil
}

type relatedImage struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

func Bundle(ctx context.Context, target oras.Target, bundleDir, version string) (*ocispec.Descriptor, int64, error) {
	var (
		bytesPushed   int64
		relatedImages []relatedImage
		descs         []ocispec.Descriptor
	)

	relatedImagesYAML, err := os.ReadFile(filepath.Join(bundleDir, "related_images.yaml"))
	if err != nil {
		return nil, bytesPushed, err
	}
	if err := yaml.Unmarshal(relatedImagesYAML, &relatedImages); err != nil {
		return nil, bytesPushed, err
	}
	for _, ri := range relatedImages {
		desc, imageBytesPushed, err := pushImageRef(ctx, target, ri.Name, ri.Image)
		bytesPushed += imageBytesPushed
		if err != nil {
			return nil, bytesPushed, err
		}
		descs = append(descs, *desc)
	}

	bundleContent := &bytes.Buffer{}
	gzw := gzip.NewWriter(bundleContent)
	if err := tarDirectory(filepath.Join(bundleDir, "content"), gzw); err != nil {
		return nil, bytesPushed, err
	}
	if err := gzw.Close(); err != nil {
		return nil, bytesPushed, err
	}
	bundleContentDesc := ocispec.Descriptor{
		MediaType: v0.MediaTypeCNCFOperatorFrameworkBundleContentPlainV0TarGZ,
		Digest:    digest.FromBytes(bundleContent.Bytes()),
		Size:      int64(bundleContent.Len()),
	}
	contentBytesPushed, err := pushIfNotExist(ctx, target, bundleContentDesc, io.NopCloser(bundleContent), nil)
	bytesPushed += contentBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}

	properties, err := os.ReadFile(filepath.Join(bundleDir, "properties.yaml"))
	if err != nil {
		return nil, bytesPushed, err
	}
	propertiesDesc := ocispec.Descriptor{
		MediaType: v0.MediaTypeCNCFOperatorFrameworkPropertiesV0YAML,
		Digest:    digest.FromBytes(properties),
		Size:      int64(len(properties)),
	}
	propertiesBytesPushed, err := pushIfNotExist(ctx, target, propertiesDesc, io.NopCloser(bytes.NewReader(properties)), nil)
	bytesPushed += propertiesBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}

	constraints, err := os.ReadFile(filepath.Join(bundleDir, "constraints.yaml"))
	if err != nil {
		return nil, bytesPushed, err
	}
	constraintsDesc := ocispec.Descriptor{
		MediaType: v0.MediaTypeCNCFOperatorFrameworkConstraintsV0YAML,
		Digest:    digest.FromBytes(constraints),
		Size:      int64(len(constraints)),
	}
	constraintsBytesPushed, err := pushIfNotExist(ctx, target, constraintsDesc, io.NopCloser(bytes.NewReader(constraints)), nil)
	bytesPushed += constraintsBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}

	//artifact, _ := json.Marshal(ocispec.Artifact{
	//	MediaType:    ocispec.MediaTypeArtifactManifest,
	//	ArtifactType: v0.MediaTypeCNCFOperatorFrameworkBundleV0,
	//	Blobs:        append([]ocispec.Descriptor{bundleConstraintsDesc, bundleContentDesc}, descs...),
	//	Annotations:  map[string]string{"tag": tag},
	//})
	//artifactDesc := ocispec.Descriptor{
	//	MediaType:    ocispec.MediaTypeArtifactManifest,
	//	ArtifactType: v0.MediaTypeCNCFOperatorFrameworkBundleV0,
	//	Digest:       digest.FromBytes(artifact),
	//	Size:         int64(len(artifact)),
	//	Annotations:  map[string]string{"tag": tag},
	//}

	artifact, _ := json.Marshal(ocispec.Index{
		Versioned:   specs.Versioned{SchemaVersion: 2},
		MediaType:   ocispec.MediaTypeImageIndex,
		Manifests:   append([]ocispec.Descriptor{propertiesDesc, constraintsDesc, bundleContentDesc}, descs...),
		Annotations: map[string]string{"artifactType": v0.MediaTypeCNCFOperatorFrameworkBundleV0},
	})
	artifactDesc := ocispec.Descriptor{
		MediaType:    ocispec.MediaTypeImageIndex,
		ArtifactType: v0.MediaTypeCNCFOperatorFrameworkBundleV0,
		Digest:       digest.FromBytes(artifact),
		Size:         int64(len(artifact)),
		Annotations:  map[string]string{"artifactType": v0.MediaTypeCNCFOperatorFrameworkBundleV0},
	}

	artifactBytesPushed, err := pushIfNotExist(ctx, target, artifactDesc, io.NopCloser(bytes.NewReader(artifact)), tag(fmt.Sprintf("bundle.%s", version)))
	bytesPushed += artifactBytesPushed
	if err != nil {
		return nil, bytesPushed, err
	}
	return &artifactDesc, bytesPushed, nil
}

type tag string

func (t tag) String() string {
	return t.Tag()
}
func (t tag) Tag() string {
	return string(t)
}

func pushImageRef(ctx context.Context, target oras.Target, imageName, imageRef string) (*ocispec.Descriptor, int64, error) {
	src, ref, desc, err := util.ResolveNameAndReference(ctx, imageRef)
	if err != nil {
		return nil, 0, err
	}

	if nt, ok := ref.(reference.NamedTagged); ok {
		ref, err = reference.WithTag(nt, fmt.Sprintf("image.%s.%s", imageName, nt.Tag()))
		if err != nil {
			return nil, 0, err
		}
	}

	copyBytes, err := copy.Descriptor(ctx, target, src, *desc, ref)
	if err != nil {
		return nil, copyBytes, err
	}

	return desc, copyBytes, nil
}

type singleUseStore struct {
	desc   ocispec.Descriptor
	reader io.ReadCloser
}

func (s *singleUseStore) Fetch(_ context.Context, d ocispec.Descriptor) (io.ReadCloser, error) {
	return s.reader, nil
}

func pushIfNotExist(ctx context.Context, target oras.Target, d ocispec.Descriptor, blob io.ReadCloser, ref reference.Reference) (int64, error) {
	s := &singleUseStore{d, blob}
	return copy.Descriptor(ctx, target, s, d, ref)
}

func tarDirectory(root string, w io.Writer) (err error) {
	tw := tar.NewWriter(w)
	defer func() {
		closeErr := tw.Close()
		if err == nil {
			err = closeErr
		}
	}()

	return filepath.Walk(root, func(path string, info os.FileInfo, err error) (returnErr error) {
		if err != nil {
			return err
		}

		// Rename path
		name, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name = filepath.ToSlash(name)

		// Generate header
		var link string
		mode := info.Mode()
		if mode&os.ModeSymlink != 0 {
			if link, err = os.Readlink(path); err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		header.Name = name
		header.Uid = 0
		header.Gid = 0
		header.Uname = ""
		header.Gname = ""

		header.ModTime = time.Time{}
		header.AccessTime = time.Time{}
		header.ChangeTime = time.Time{}

		// Write file
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("tar: %w", err)
		}
		if mode.IsRegular() {
			fp, err := os.Open(path)
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
		}

		return nil
	})
}
