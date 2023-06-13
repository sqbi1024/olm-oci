package pkg

import (
	"bytes"
	"compress/gzip"
	"context"
	gojson "encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/opencontainers/go-digest"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/property"
	"k8s.io/apimachinery/pkg/util/sets"
	"oras.land/oras-go/v2/content/memory"

	"github.com/joelanford/olm-oci/internal/client"
	"github.com/joelanford/olm-oci/internal/json"
	"github.com/joelanford/olm-oci/internal/tar"
)

const (
	AnnotationKeyArtifactType  = "io.operatorframework.artifact-type"
	AnnotationKeyName          = "io.operatorframework.name"
	AnnotationKeyBundleVersion = "io.operatorframework.bundle.version"
	AnnotationKeyBundleRelease = "io.operatorframework.bundle.release"

	MediaTypePackage         = "application/vnd.cncf.operatorframework.olm.package.v1"
	MediaTypePackageMetadata = "application/vnd.cncf.operatorframework.olm.package.metadata.v1+json"
	MediaTypeUpgradeEdges    = "application/vnd.cncf.operatorframework.olm.upgrade-edges.v1+json"

	MediaTypeChannel         = "application/vnd.cncf.operatorframework.olm.channel.v1"
	MediaTypeChannelMetadata = "application/vnd.cncf.operatorframework.olm.channel.metadata.v1+json"

	MediaTypeBundle                 = "application/vnd.cncf.operatorframework.olm.bundle.v1"
	MediaTypeBundleMetadata         = "application/vnd.cncf.operatorframework.olm.bundle.metadata.v1+json"
	MediaTypeBundleFormatPlainV0    = "application/vnd.cncf.operatorframework.olm.bundle.format.plain.v0.tar+gzip"
	MediaTypeBundleFormatRegistryV1 = "application/vnd.cncf.operatorframework.olm.bundle.format.registry.v1.tar+gzip"

	MediaTypeProperties  = "application/vnd.cncf.operatorframework.olm.properties.v1+json"
	MediaTypeConstraints = "application/vnd.cncf.operatorframework.olm.constraints.v1+json"
)

var (
	_ client.Artifact = &Package{}
	_ client.Artifact = &Channel{}
	_ client.Artifact = &Bundle{}

	_ client.Blob = &PackageMetadata{}
	_ client.Blob = Description("")
	_ client.Blob = &Icon{}
	_ client.Blob = UpgradeEdges{}
	_ client.Blob = &ChannelMetadata{}
	_ client.Blob = &Properties{}
	_ client.Blob = &Constraints{}
)

type Package struct {
	Metadata     PackageMetadata
	Description  Description
	Icon         *Icon
	UpgradeEdges UpgradeEdges
	Properties   Properties

	Channels []Channel
}

func LoadPackage(packageDir string) (*Package, error) {
	var (
		pkg Package
		err error
	)

	pkg.Metadata, err = loadPackageMetadata(filepath.Join(packageDir, "package.json"))
	if err != nil {
		return nil, fmt.Errorf("error loading metadata: %w", err)
	}
	pkg.Description, err = loadDescription(filepath.Join(packageDir, "README.md"))
	if err != nil {
		return nil, fmt.Errorf("error loading description: %w", err)
	}
	pkg.Icon, err = loadIcon(packageDir)
	if err != nil {
		return nil, fmt.Errorf("error loading icon: %w", err)
	}

	bundles, err := loadBundles(packageDir)
	if err != nil {
		return nil, fmt.Errorf("error loading bundles: %w", err)
	}

	pkg.UpgradeEdges, err = loadUpgradeEdges(packageDir, bundles)
	if err != nil {
		return nil, fmt.Errorf("error loading upgrade edges: %w", err)
	}
	pkg.Properties, err = loadProperties(packageDir)
	if err != nil {
		return nil, fmt.Errorf("error loading properties: %w", err)
	}
	pkg.Channels, err = loadChannels(packageDir, bundles)
	if err != nil {
		return nil, fmt.Errorf("error loading channels: %w", err)
	}

	return &pkg, nil
}

func loadPackageMetadata(metadataFile string) (PackageMetadata, error) {
	data, err := os.ReadFile(metadataFile)
	if err != nil {
		return PackageMetadata{}, err
	}
	var metadata PackageMetadata
	err = gojson.Unmarshal(data, &metadata)
	return metadata, err
}

func loadDescription(file string) (Description, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	return Description(data), nil
}

func loadIcon(packageDir string) (*Icon, error) {
	var (
		file      string
		mediaType string
	)
	svgFile := filepath.Join(packageDir, "icon.svg")
	pngFile := filepath.Join(packageDir, "icon.png")
	if _, err := os.Stat(svgFile); err == nil {
		file, mediaType = svgFile, "image/svg+xml"
	} else if _, err := os.Stat(pngFile); err == nil {
		file, mediaType = pngFile, "image/png"
	} else {
		return nil, nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return &Icon{ImageData: data, ImageMediaType: mediaType}, nil
}

func loadUpgradeEdges(packageDir string, bundles []Bundle) (UpgradeEdges, error) {
	data, err := os.ReadFile(filepath.Join(packageDir, "upgrade-edges.json"))
	if err != nil {
		return nil, err
	}
	var upgradeEdges UpgradeEdges
	if err = gojson.Unmarshal(data, &upgradeEdges); err != nil {
		return nil, err
	}

	byVersionBundles := map[string][]Bundle{}
	for _, bundle := range bundles {
		byVersionBundles[bundle.Metadata.Version.String()] = append(byVersionBundles[bundle.Metadata.Version.String()], bundle)
	}
	for version, releases := range byVersionBundles {
		sort.Slice(releases, func(i, j int) bool {
			return releases[i].Metadata.Release < releases[j].Metadata.Release
		})
		byVersionBundles[version] = releases
	}

	fullVersion := func(release Bundle) string {
		return fmt.Sprintf("%s-%d", release.Metadata.Version, release.Metadata.Release)
	}
	fullVersions := func(releases []Bundle) []string {
		fullVersions := make([]string, len(releases))
		for i, release := range releases {
			fullVersions[i] = fullVersion(release)
		}
		return fullVersions
	}

	byVersion := map[string][]string{}
	for version, releases := range byVersionBundles {
		byVersion[version] = fullVersions(releases)
	}

	finalUpgradeEdges := map[string][]string{}
	for fromVersion, toVersions := range upgradeEdges {
		for i, fromRelease := range byVersion[fromVersion] {
			finalUpgradeEdges[fromRelease] = byVersion[fromVersion][i+1:]
			for _, toVersion := range toVersions {
				finalUpgradeEdges[fromRelease] = append(finalUpgradeEdges[fromRelease], byVersion[toVersion][len(byVersion[toVersion])-1])
			}
			sort.Sort(sort.Reverse(sort.StringSlice(finalUpgradeEdges[fromRelease])))
		}
	}
	return finalUpgradeEdges, err
}

func loadProperties(packageDir string) (Properties, error) {
	data, err := os.ReadFile(filepath.Join(packageDir, "properties.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var properties Properties
	err = gojson.Unmarshal(data, &properties)
	return properties, err
}

func loadConstraints(packageDir string) (Constraints, error) {
	data, err := os.ReadFile(filepath.Join(packageDir, "constraints.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var constraints Constraints
	err = gojson.Unmarshal(data, &constraints)
	return constraints, err
}

func loadBundles(packageDir string) ([]Bundle, error) {
	bundlesDir := filepath.Join(packageDir, "bundles")
	entries, err := os.ReadDir(bundlesDir)
	if err != nil {
		return nil, err
	}
	var bundles []Bundle
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		bundleDir := filepath.Join(bundlesDir, entry.Name())
		bundle, err := LoadBundle(bundleDir)
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, *bundle)
	}
	return bundles, nil
}

func loadChannels(packageDir string, bundles []Bundle) ([]Channel, error) {
	channelsDir := filepath.Join(packageDir, "channels")
	entries, err := os.ReadDir(channelsDir)
	if err != nil {
		return nil, err
	}
	var channels []Channel
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		channelDir := filepath.Join(channelsDir, entry.Name())
		channel, err := LoadChannel(channelDir, bundles)
		if err != nil {
			return nil, err
		}
		channels = append(channels, *channel)
	}
	return channels, nil
}

func LoadBundle(bundleDir string) (*Bundle, error) {
	var (
		bundle Bundle
		err    error
	)

	bundle.Metadata, err = loadBundleMetadata(filepath.Join(bundleDir, "bundle.json"))
	if err != nil {
		return nil, fmt.Errorf("error loading metadata: %w", err)
	}
	bundle.Properties, err = loadProperties(bundleDir)
	if err != nil {
		return nil, fmt.Errorf("error loading properties: %w", err)
	}
	bundle.Constraints, err = loadConstraints(bundleDir)
	if err != nil {
		return nil, fmt.Errorf("error loading constraints: %w", err)
	}
	bundle.Content, err = loadContent(bundleDir)
	if err != nil {
		return nil, fmt.Errorf("error loading content: %w", err)
	}

	return &bundle, nil
}

func loadBundleMetadata(metadataFile string) (BundleMetadata, error) {
	data, err := os.ReadFile(metadataFile)
	if err != nil {
		return BundleMetadata{}, err
	}
	var metadata BundleMetadata
	err = gojson.Unmarshal(data, &metadata)
	return metadata, err
}

func loadContent(bundleDir string) (BundleContent, error) {
	contentDir := filepath.Join(bundleDir, "content")
	contentFS := os.DirFS(contentDir)

	mediaTypeFilePath := filepath.Join(bundleDir, "media-type")

	if mediaTypeFile, err := contentFS.Open(mediaTypeFilePath); err == nil {
		defer mediaTypeFile.Close()
		mediaTypeBytes, err := io.ReadAll(mediaTypeFile)
		if err != nil {
			return BundleContent{}, err
		}
		return BundleContent{MediaTyp: string(mediaTypeBytes), FS: os.DirFS(contentDir)}, nil
	}

	entries, err := fs.ReadDir(contentFS, ".")
	if err != nil {
		return BundleContent{}, err
	}
	if len(entries) == 1 && entries[0].IsDir() && entries[0].Name() == "manifests" {
		return BundleContent{MediaTyp: MediaTypeBundleFormatPlainV0, FS: os.DirFS(contentDir)}, nil
	}

	if len(entries) == 2 &&
		entries[0].IsDir() && entries[0].Name() == "manifests" &&
		entries[1].IsDir() && entries[1].Name() == "metadata" {
		return BundleContent{MediaTyp: MediaTypeBundleFormatRegistryV1, FS: os.DirFS(contentDir)}, nil
	}
	return BundleContent{}, fmt.Errorf("unable to detect bundle content mediatype, create file %q containing the media type", mediaTypeFilePath)
}

func LoadChannel(channelDir string, bundles []Bundle) (*Channel, error) {
	var (
		channel Channel
		err     error
	)

	channel.Metadata, err = loadChannelMetadata(filepath.Join(channelDir, "channel.json"))
	if err != nil {
		return nil, fmt.Errorf("error loading metadata: %w", err)
	}
	channel.Bundles, err = loadEntries(channelDir, bundles)
	if err != nil {
		return nil, fmt.Errorf("error loading entries: %w", err)
	}
	channel.Properties, err = loadProperties(channelDir)
	if err != nil {
		return nil, fmt.Errorf("error loading properties: %w", err)
	}

	return &channel, nil
}

func loadEntries(channelDir string, bundles []Bundle) ([]Bundle, error) {
	data, err := os.ReadFile(filepath.Join(channelDir, "entries.json"))
	if err != nil {
		return nil, err
	}
	var entries []string
	if err := gojson.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	byVersion := map[string][]Bundle{}
	for _, bundle := range bundles {
		byVersion[bundle.Metadata.Version.String()] = append(byVersion[bundle.Metadata.Version.String()], bundle)
	}

	var out []Bundle
	for _, entry := range entries {
		bundlesByVersion, ok := byVersion[entry]
		if !ok {
			return nil, fmt.Errorf("no bundles found with version %q", entry)
		}
		out = append(out, bundlesByVersion...)
	}
	return out, nil
}

func loadChannelMetadata(metadataFile string) (ChannelMetadata, error) {
	data, err := os.ReadFile(metadataFile)
	if err != nil {
		return ChannelMetadata{}, err
	}
	var metadata ChannelMetadata
	err = gojson.Unmarshal(data, &metadata)
	return metadata, err
}

func (p Package) ArtifactType() string {
	return MediaTypePackage
}

func (p Package) Annotations() map[string]string {
	return map[string]string{AnnotationKeyName: p.Metadata.Name}
}

func (p Package) SubIndices() []client.Artifact {
	var indices []client.Artifact
	for _, ch := range p.Channels {
		indices = append(indices, ch)
	}
	return indices
}

func (p Package) Blobs() []client.Blob {
	blobs := []client.Blob{p.Metadata}
	if p.Description != "" {
		blobs = append(blobs, p.Description)
	}
	if p.Icon != nil {
		blobs = append(blobs, p.Icon)
	}
	if len(p.UpgradeEdges) > 0 {
		blobs = append(blobs, p.UpgradeEdges)
	}
	if len(p.Properties) > 0 {
		blobs = append(blobs, p.Properties)
	}
	return blobs
}

type PackageMetadata struct {
	Name        string       `json:"name"`
	DisplayName string       `json:"displayName,omitempty"`
	Keywords    []string     `json:"keywords,omitempty"`
	URLs        []string     `json:"urls,omitempty"`
	Maintainers []Maintainer `json:"maintainers,omitempty"`
	Maturity    string       `json:"maturity,omitempty"`
}

type Maintainer struct {
	Email string `json:"email"`
	Name  string `json:"name,omitempty"`
}

func (pm PackageMetadata) MediaType() string {
	return MediaTypePackageMetadata
}

func (pm PackageMetadata) Data() (io.ReadCloser, error) {
	data, err := json.Marshal(pm)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type Description string

func (d Description) MediaType() string {
	return "text/markdown"
}

func (d Description) Data() (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(string(d))), nil
}

type Icon struct {
	ImageData      []byte
	ImageMediaType string
}

func (i Icon) MediaType() string {
	return i.ImageMediaType
}

func (i Icon) Data() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(i.ImageData)), nil
}

type UpgradeEdges map[string][]string

func (ue UpgradeEdges) MediaType() string {
	return MediaTypeUpgradeEdges
}

func (ue UpgradeEdges) Data() (io.ReadCloser, error) {
	data, err := json.Marshal(ue)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type Channel struct {
	Metadata   ChannelMetadata
	Properties Properties

	Bundles []Bundle
}

func (c Channel) ArtifactType() string {
	return MediaTypeChannel
}

func (c Channel) Annotations() map[string]string {
	return map[string]string{AnnotationKeyName: c.Metadata.Name}
}

func (c Channel) SubIndices() []client.Artifact {
	var indices []client.Artifact
	for _, b := range c.Bundles {
		indices = append(indices, b)
	}
	return indices
}

func (c Channel) Blobs() []client.Blob {
	blobs := []client.Blob{c.Metadata}
	if len(c.Properties) > 0 {
		blobs = append(blobs, c.Properties)
	}
	return blobs
}

type ChannelMetadata struct {
	Name string `json:"name"`
}

func (cm ChannelMetadata) MediaType() string {
	return MediaTypeChannelMetadata
}

func (cm ChannelMetadata) Data() (io.ReadCloser, error) {
	data, err := json.Marshal(cm)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type Bundle struct {
	Metadata    BundleMetadata
	Properties  Properties
	Constraints Constraints
	Content     BundleContent
}

func (b Bundle) ArtifactType() string {
	return MediaTypeBundle
}

func (b Bundle) Annotations() map[string]string {
	return map[string]string{
		AnnotationKeyBundleVersion: b.Metadata.Version.String(),
		AnnotationKeyBundleRelease: fmt.Sprintf("%d", b.Metadata.Release),
	}
}

func (b Bundle) SubIndices() []client.Artifact {
	return nil
}

func (b Bundle) Blobs() []client.Blob {
	blobs := []client.Blob{b.Metadata, b.Content}
	if len(b.Properties) > 0 {
		blobs = append(blobs, b.Properties)
	}
	if len(b.Constraints) > 0 {
		blobs = append(blobs, b.Constraints)
	}
	return blobs
}

type BundleMetadata struct {
	Package string         `json:"package"`
	Version semver.Version `json:"version"`
	Release uint           `json:"release"`
}

func (bm BundleMetadata) MediaType() string {
	return MediaTypeBundleMetadata
}

func (bm BundleMetadata) Data() (io.ReadCloser, error) {
	data, err := json.Marshal(bm)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type BundleContent struct {
	MediaTyp string
	FS       fs.FS
}

func (bc BundleContent) MediaType() string {
	return bc.MediaTyp
}

func (bc BundleContent) Data() (io.ReadCloser, error) {
	buf := bytes.NewBuffer(nil)
	gzw := gzip.NewWriter(buf)
	defer gzw.Close()
	if err := tar.WriteFS(bc.FS, gzw); err != nil {
		return nil, fmt.Errorf("error creating bundle content: %w", err)
	}
	return io.NopCloser(buf), nil
}

type Properties TypeValues

func (p Properties) MediaType() string {
	return MediaTypeProperties
}
func (p Properties) Data() (io.ReadCloser, error) {
	return TypeValues(p).Data()
}

type Constraints TypeValues

func (c Constraints) MediaType() string {
	return MediaTypeConstraints
}
func (c Constraints) Data() (io.ReadCloser, error) {
	return TypeValues(c).Data()
}

type TypeValues []TypeValue

type TypeValue struct {
	Type  string            `json:"type"`
	Value gojson.RawMessage `json:"value"`
}

func (tv TypeValues) Data() (io.ReadCloser, error) {
	data, err := json.Marshal(tv)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func convertTypeValues(in []TypeValue) []property.Property {
	out := make([]property.Property, len(in))
	for i, tv := range in {
		out[i] = property.Property{
			Type:  tv.Type,
			Value: tv.Value,
		}
	}
	return out
}

func (p Package) ToFBC(ctx context.Context, repo, defaultChannel string) (*declcfg.DeclarativeConfig, error) {
	pkg := declcfg.Package{
		Schema:         "olm.schema",
		Name:           p.Metadata.Name,
		DefaultChannel: defaultChannel,
		Description:    string(p.Description),
		Properties:     convertTypeValues(p.Properties),
	}
	if p.Icon != nil {
		pkg.Icon = &declcfg.Icon{
			Data:      p.Icon.ImageData,
			MediaType: p.Icon.ImageMediaType,
		}
	}

	fullVersion := func(b Bundle) string {
		return fmt.Sprintf("%s-%d", b.Metadata.Version, b.Metadata.Release)
	}
	bundleName := func(b Bundle) string {
		return fmt.Sprintf("%s.v%s", p.Metadata.Name, fullVersion(b))
	}

	channels := make([]declcfg.Channel, 0, len(p.Channels))
	bundleMap := map[string]declcfg.Bundle{}
	for _, ch := range p.Channels {
		inChannel := sets.New[string]()
		lookup := make(map[string]Bundle)
		for _, b := range ch.Bundles {
			inChannel.Insert(fullVersion(b))
			lookup[fullVersion(b)] = b
		}

		entries := make([]declcfg.ChannelEntry, 0, len(ch.Bundles))
		for _, b := range ch.Bundles {
			from := fullVersion(b)
			tos := p.UpgradeEdges[from]
			for _, to := range tos {
				if !inChannel.Has(to) {
					continue
				}
				entries = append(entries, declcfg.ChannelEntry{
					Name:     bundleName(lookup[to]),
					Replaces: bundleName(lookup[from]),
				})
			}
		}

		channels = append(channels, declcfg.Channel{
			Schema:     "olm.channel",
			Package:    p.Metadata.Name,
			Name:       ch.Metadata.Name,
			Entries:    entries,
			Properties: convertTypeValues(ch.Properties),
		})

		for _, b := range ch.Bundles {
			dig, err := b.getDigest(ctx)
			if err != nil {
				return nil, err
			}
			bundleMap[fullVersion(b)] = declcfg.Bundle{
				Schema:     "olm.bundle",
				Package:    p.Metadata.Name,
				Name:       bundleName(b),
				Image:      fmt.Sprintf("%s@%s", repo, dig),
				Properties: append(convertTypeValues(b.Properties), convertTypeValues(b.Constraints)...),
			}
		}
	}

	bundles := make([]declcfg.Bundle, 0, len(bundleMap))
	for _, b := range bundleMap {
		bundles = append(bundles, b)
	}
	sort.Slice(bundles, func(i, j int) bool {
		return bundles[i].Name < bundles[j].Name
	})

	return &declcfg.DeclarativeConfig{
		Packages: []declcfg.Package{pkg},
		Channels: channels,
		Bundles:  bundles,
	}, nil
}

func (b Bundle) getDigest(ctx context.Context) (digest.Digest, error) {
	st := memory.New()
	desc, err := client.Push(ctx, b, st)
	if err != nil {
		return "", err
	}
	return desc.Digest, nil
}
