package inspect

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strings"

	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/nlepage/go-tarfs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"oras.land/oras-go/v2"

	"github.com/joelanford/olm-oci/internal/pkg"
)

func Inspect(ctx context.Context, repo oras.Target, desc ocispec.Descriptor) error {
	return inspect(ctx, repo, desc, "")
}

func inspect(ctx context.Context, target oras.Target, d ocispec.Descriptor, indent string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fmt.Printf("%s- Media Type: %v\n", indent, d.MediaType)
	fmt.Printf("%s  Digest: %v\n", indent, d.Digest)
	fmt.Printf("%s  Size: %v\n", indent, d.Size)

	rc, err := target.Fetch(ctx, d)
	if err != nil {
		return err
	}
	defer rc.Close()

	switch d.MediaType {
	case pkg.MediaTypeUpgradeEdges:
		data, err := io.ReadAll(rc)
		if err != nil {
			return err
		}
		var edges pkg.UpgradeEdges
		if err := yaml.Unmarshal(data, &edges); err != nil {
			return err
		}
		fmt.Printf("%s  Upgrade Edges:\n", indent)
		for from, to := range edges {
			fmt.Printf("%s    - From: %s\n", indent, from)
			fmt.Printf("%s      To: %s\n", indent, strings.Join(to, ", "))
		}
	case ocispec.MediaTypeArtifactManifest:
		var a ocispec.Artifact
		if err := json.NewDecoder(rc).Decode(&a); err != nil {
			return err
		}
		fmt.Printf("%s  Artifact Type: %v\n", indent, a.ArtifactType)
		fmt.Printf("%s  Artifact Annotations: %#v\n", indent, a.Annotations)
		fmt.Printf("%s  Artifact Blobs:\n", indent)
		for _, blob := range a.Blobs {
			if err := inspect(ctx, target, blob, fmt.Sprintf("%s    ", indent)); err != nil {
				return err
			}
		}
	case pkg.MediaTypePackageMetadata:
		data, err := io.ReadAll(rc)
		if err != nil {
			return err
		}
		var m pkg.PackageMetadata
		if err := yaml.Unmarshal(data, &m); err != nil {
			return err
		}
		fmt.Printf("%s  Package Metadata:\n", indent)
		fmt.Printf("%s    Name: %s\n", indent, m.Name)
		if m.DisplayName != "" {
			fmt.Printf("%s    DisplayName: %s\n", indent, m.DisplayName)
		}
		if len(m.Keywords) > 0 {
			fmt.Printf("%s    Keywords: %s\n", indent, m.Keywords)
		}
		if len(m.URLs) > 0 {
			fmt.Printf("%s    URLs: %s\n", indent, m.URLs)
		}
		if len(m.Maintainers) > 0 {
			fmt.Printf("%s    Maintainers: %s\n", indent, m.Maintainers)
		}
	case pkg.MediaTypeChannelMetadata:
		data, err := io.ReadAll(rc)
		if err != nil {
			return err
		}
		var m pkg.ChannelMetadata
		if err := yaml.Unmarshal(data, &m); err != nil {
			return err
		}
		fmt.Printf("%s  Channel Metadata:\n", indent)
		fmt.Printf("%s    Name: %s\n", indent, m.Name)
	case pkg.MediaTypeBundleMetadata:
		data, err := io.ReadAll(rc)
		if err != nil {
			return err
		}
		var m pkg.BundleMetadata
		if err := yaml.Unmarshal(data, &m); err != nil {
			return err
		}
		fmt.Printf("%s  Bundle Metadata:\n", indent)
		fmt.Printf("%s    Package: %s\n", indent, m.Package)
		fmt.Printf("%s    Version: %s\n", indent, m.Version)
		fmt.Printf("%s    Release: %d\n", indent, m.Release)
	case pkg.MediaTypeBundleContent:
		gzr, err := gzip.NewReader(rc)
		if err != nil {
			return fmt.Errorf("read gzip: %v", err)
		}
		tfs, err := tarfs.New(gzr)
		if err != nil {
			return fmt.Errorf("read tar: %v", err)
		}
		fmt.Printf("%s  Bundle Content:\n", indent)
		if err := fs.WalkDir(tfs, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			stat, err := d.Info()
			if err != nil {
				return err
			}
			fmt.Printf("%s    - Path: %s\n", indent, path)
			fmt.Printf("%s      Mode: %s\n", indent, stat.Mode())
			fmt.Printf("%s      Size: %d\n", indent, stat.Size())

			return nil
		}); err != nil {
			return err
		}
	case pkg.MediaTypeProperties:
		data, err := io.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("read gzip: %v", err)
		}
		fmt.Printf("%s  Properties:\n", indent)
		fmt.Printf("%s\n", regexp.MustCompile("(?m)^").ReplaceAllString(string(data), fmt.Sprintf("%s    ", indent)))
	case pkg.MediaTypeConstraints:
		data, err := io.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("read gzip: %v", err)
		}
		fmt.Printf("%s  Constraints:\n", indent)
		fmt.Printf("%s\n", regexp.MustCompile("(?m)^").ReplaceAllString(string(data), fmt.Sprintf("%s    ", indent)))
	case ocispec.MediaTypeImageIndex:
		var i ocispec.Index
		if err := json.NewDecoder(rc).Decode(&i); err != nil {
			return err
		}
		fmt.Printf("%s  Image Index Annotations: %#v\n", indent, i.Annotations)
		fmt.Printf("%s  Image Index Manifests:\n", indent)
		for _, blob := range i.Manifests {
			if err := inspect(ctx, target, blob, fmt.Sprintf("%s    ", indent)); err != nil {
				return err
			}
		}
	case manifestlist.MediaTypeManifestList:
		var m manifestlist.ManifestList
		if err := json.NewDecoder(rc).Decode(&m); err != nil {
			return err
		}
		fmt.Printf("%s  Manifest List Manifests:\n", indent)
		for _, blob := range m.Manifests {
			desc := ocispec.Descriptor{
				MediaType:   blob.MediaType,
				Digest:      blob.Digest,
				Size:        blob.Size,
				URLs:        blob.URLs,
				Annotations: blob.Annotations,
				Platform: &ocispec.Platform{
					Architecture: blob.Platform.Architecture,
					OS:           blob.Platform.OS,
					OSVersion:    blob.Platform.OSVersion,
					OSFeatures:   blob.Platform.OSFeatures,
					Variant:      blob.Platform.Variant,
				},
			}
			if err := inspect(ctx, target, desc, fmt.Sprintf("%s    ", indent)); err != nil {
				return err
			}
		}
	case ocispec.MediaTypeImageManifest:
		var m ocispec.Manifest
		if err := json.NewDecoder(rc).Decode(&m); err != nil {
			return err
		}
		fmt.Printf("%s  Image Config:\n", indent)
		if err := inspect(ctx, target, m.Config, fmt.Sprintf("%s    ", indent)); err != nil {
			return err
		}
		fmt.Printf("%s  Image Manifest Layers:\n", indent)
		for _, blob := range m.Layers {
			if err := inspect(ctx, target, blob, fmt.Sprintf("%s    ", indent)); err != nil {
				return err
			}
		}
	case schema2.MediaTypeManifest:
		var m schema2.Manifest
		if err := json.NewDecoder(rc).Decode(&m); err != nil {
			return err
		}
		configDesc := ocispec.Descriptor{
			MediaType:   m.Config.MediaType,
			Digest:      m.Config.Digest,
			Size:        m.Config.Size,
			URLs:        m.Config.URLs,
			Annotations: m.Config.Annotations,
			Platform:    m.Config.Platform,
		}
		fmt.Printf("%s  Image Config:\n", indent)
		if err := inspect(ctx, target, configDesc, fmt.Sprintf("%s    ", indent)); err != nil {
			return err
		}
		fmt.Printf("%s  Image Manifest Layers:\n", indent)
		for _, blob := range m.Layers {
			blobDesc := ocispec.Descriptor{
				MediaType:   blob.MediaType,
				Digest:      blob.Digest,
				Size:        blob.Size,
				URLs:        blob.URLs,
				Annotations: blob.Annotations,
				Platform:    blob.Platform,
			}
			if err := inspect(ctx, target, blobDesc, fmt.Sprintf("%s    ", indent)); err != nil {
				return err
			}
		}
	case schema2.MediaTypeLayer, ocispec.MediaTypeImageLayer:
		gzr, err := gzip.NewReader(rc)
		if err != nil {
			return fmt.Errorf("read gzip: %v", err)
		}
		tfs, err := tarfs.New(gzr)
		if err != nil {
			return fmt.Errorf("read tar: %v", err)
		}
		fmt.Printf("%s  File Content:\n", indent)
		if err := fs.WalkDir(tfs, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			stat, err := d.Info()
			if err != nil {
				return err
			}
			fmt.Printf("%s    - Path: %s\n", indent, path)
			fmt.Printf("%s      Mode: %s\n", indent, stat.Mode())
			fmt.Printf("%s      Size: %d\n", indent, stat.Size())

			return nil
		}); err != nil {
			return err
		}
	case ocispec.MediaTypeImageConfig, schema2.MediaTypeImageConfig:
		var c ocispec.Image
		if err := json.NewDecoder(rc).Decode(&c); err != nil {
			return err
		}
		fmt.Printf("%s  Author: %s\n", indent, c.Author)
		if c.Created != nil {
			fmt.Printf("%s  Created: %s\n", indent, c.Created)
		}
		fmt.Printf("%s  OS: %s\n", indent, c.OS)
		if c.OSVersion != "" {
			fmt.Printf("%s  OS Version: %s\n", indent, c.OSVersion)
		}
		if len(c.OSFeatures) > 0 {
			fmt.Printf("%s  OS Features: [%s]\n", indent, strings.Join(c.OSFeatures, ","))
		}
		fmt.Printf("%s  Architecture: %s\n", indent, c.Architecture)
		fmt.Printf("%s  RootFS:\n", indent)
		fmt.Printf("%s      Type: %s\n", indent, c.RootFS.Type)
		fmt.Printf("%s      DiffIDs:\n", indent)
		for _, id := range c.RootFS.DiffIDs {
			fmt.Printf("%s          %s\n", indent, id)
		}
		fmt.Printf("%s  Config:\n", indent)
		if len(c.Config.Labels) > 0 {
			fmt.Printf("%s      Labels: %s\n", indent, c.Config.Labels)
		}
		fmt.Printf("%s      User: %s\n", indent, c.Config.User)
		if len(c.Config.Cmd) > 0 {
			fmt.Printf("%s      Cmd: %s\n", indent, c.Config.Cmd)
		}
		fmt.Printf("%s      Env:\n", indent)
		for _, env := range c.Config.Env {
			fmt.Printf("%s          %s\n", indent, env)
		}
		fmt.Printf("%s      Entrypoint: %s\n", indent, c.Config.Entrypoint)
		if len(c.Config.ExposedPorts) > 0 {
			fmt.Printf("%s      ExposedPorts: %s\n", indent, c.Config.ExposedPorts)
		}
		fmt.Printf("%s      WorkingDir: %s\n", indent, c.Config.WorkingDir)
		if len(c.Config.Volumes) > 0 {
			fmt.Printf("%s      Volumes: %s\n", indent, c.Config.Volumes)
		}
		if c.Config.StopSignal != "" {
			fmt.Printf("%s      StopSignal: %s\n", indent, c.Config.StopSignal)
		}

	}
	return nil
}
