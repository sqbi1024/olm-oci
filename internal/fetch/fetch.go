package fetch

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strings"

	"github.com/nlepage/go-tarfs"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2"

	v0 "github.com/joelanford/olm-oci/internal/api/v0"
	"github.com/joelanford/olm-oci/internal/util"
)

func Fetch(ctx context.Context, refStr string) error {
	repo, _, desc, err := util.ResolveNameAndReference(ctx, refStr)
	if err != nil {
		return err
	}
	if err := printSelfAndChildren(ctx, repo, *desc, ""); err != nil {
		return err
	}
	return nil
}

func printSelfAndChildren(ctx context.Context, target oras.Target, d ocispec.Descriptor, indent string) error {
	fmt.Printf("%s- Media Type: %v\n", indent, d.MediaType)
	fmt.Printf("%s  Digest: %v\n", indent, d.Digest)

	rc, err := target.Fetch(ctx, d)
	if err != nil {
		return err
	}
	defer rc.Close()

	switch d.MediaType {
	case ocispec.MediaTypeArtifactManifest:
		var a ocispec.Artifact
		if err := json.NewDecoder(rc).Decode(&a); err != nil {
			return err
		}
		fmt.Printf("%s  Artifact Type: %v\n", indent, a.ArtifactType)
		fmt.Printf("%s  Artifact Annotations: %#v\n", indent, a.Annotations)
		fmt.Printf("%s  Artifact Blobs:\n", indent)
		for _, blob := range a.Blobs {
			if err := printSelfAndChildren(ctx, target, blob, fmt.Sprintf("%s    ", indent)); err != nil {
				return err
			}
		}
	case v0.MediaTypeCNCFOperatorFrameworkBundleContentPlainV0TarGZ:
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
	case v0.MediaTypeCNCFOperatorFrameworkPropertiesV0YAML:
		yamlData, err := io.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("read gzip: %v", err)
		}
		fmt.Printf("%s  Properties:\n", indent)
		fmt.Printf("%s\n", regexp.MustCompile("(?m)^").ReplaceAllString(string(yamlData), fmt.Sprintf("%s     ", indent)))
	case v0.MediaTypeCNCFOperatorFrameworkConstraintsV0YAML:
		yamlData, err := io.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("read gzip: %v", err)
		}
		fmt.Printf("%s  Constraints:\n", indent)
		fmt.Printf("%s\n", regexp.MustCompile("(?m)^").ReplaceAllString(string(yamlData), fmt.Sprintf("%s     ", indent)))
	case ocispec.MediaTypeImageIndex:
		var i ocispec.Index
		if err := json.NewDecoder(rc).Decode(&i); err != nil {
			return err
		}
		fmt.Printf("%s  Image Index Annotations: %#v\n", indent, i.Annotations)
		fmt.Printf("%s  Image Index Manifests:\n", indent)
		for _, blob := range i.Manifests {
			if err := printSelfAndChildren(ctx, target, blob, fmt.Sprintf("%s    ", indent)); err != nil {
				return err
			}
		}
	case ocispec.MediaTypeImageManifest:
		var m ocispec.Manifest
		if err := json.NewDecoder(rc).Decode(&m); err != nil {
			return err
		}
		fmt.Printf("%s  Image Config:\n", indent)
		if err := printSelfAndChildren(ctx, target, m.Config, fmt.Sprintf("%s    ", indent)); err != nil {
			return err
		}
		fmt.Printf("%s  Image Manifest Layers:\n", indent)
		for _, blob := range m.Layers {
			if err := printSelfAndChildren(ctx, target, blob, fmt.Sprintf("%s    ", indent)); err != nil {
				return err
			}
		}
	case ocispec.MediaTypeImageConfig:
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
