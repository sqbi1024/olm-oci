package v1

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/opencontainers/go-digest"
	"github.com/operator-framework/operator-registry/alpha/declcfg"
	"github.com/operator-framework/operator-registry/alpha/property"
	"github.com/operator-framework/operator-registry/pkg/image"
	"github.com/operator-framework/operator-registry/pkg/registry"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/sets"
	"oras.land/oras-go/v2/content/memory"
	"sigs.k8s.io/yaml"

	"github.com/joelanford/olm-oci/pkg/client"
	"github.com/joelanford/olm-oci/pkg/tar"
)

const (
	AnnotationKeyName                   = "io.operatorframework.name"
	AnnotationKeyBundlePackage          = "io.operatorframework.bundle.package"
	AnnotationKeyBundleVersion          = "io.operatorframework.bundle.version"
	AnnotationKeyBundleRelease          = "io.operatorframework.bundle.release"
	AnnotationKeyBundleContentMediaType = "io.operatorframework.bundle.content.mediatype"

	MediaTypeCatalog = "application/vnd.cncf.operatorframework.olm.catalog.v1"

	MediaTypePackage         = "application/vnd.cncf.operatorframework.olm.package.v1"
	MediaTypePackageMetadata = "application/vnd.cncf.operatorframework.olm.package.metadata.v1+yaml"
	MediaTypeUpgradeEdges    = "application/vnd.cncf.operatorframework.olm.upgrade-edges.v1+yaml"

	MediaTypeChannel         = "application/vnd.cncf.operatorframework.olm.channel.v1"
	MediaTypeChannelMetadata = "application/vnd.cncf.operatorframework.olm.channel.metadata.v1+yaml"

	MediaTypeBundle                 = "application/vnd.cncf.operatorframework.olm.bundle.v1"
	MediaTypeBundleMetadata         = "application/vnd.cncf.operatorframework.olm.bundle.metadata.v1+yaml"
	MediaTypeRelatedImages          = "application/vnd.cncf.operatorframework.olm.bundle.related-images.v1+yaml"
	MediaTypeBundleContent          = "application/vnd.cncf.operatorframework.olm.bundle.content.v1.tar+gzip"
	MediaTypeBundleFormatRegistryV1 = "registry+v1"

	MediaTypeProperties  = "application/vnd.cncf.operatorframework.olm.properties.v1+yaml"
	MediaTypeConstraints = "application/vnd.cncf.operatorframework.olm.constraints.v1+yaml"
)

var (
	_ client.Artifact = &Catalog{}
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

type Catalog struct {
	Packages []Package
}

func (c *Catalog) ArtifactType() string {
	return MediaTypeCatalog
}

func (c *Catalog) Annotations() map[string]string {
	return nil
}

func (c *Catalog) SubArtifacts() []client.Artifact {
	var artifacts []client.Artifact
	for _, p := range c.Packages {
		artifacts = append(artifacts, p)
	}
	return artifacts
}

func (c *Catalog) Blobs() []client.Blob {
	return nil
}

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

	pkg.Metadata, err = loadPackageMetadata(filepath.Join(packageDir, "package.yaml"))
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
	pkg.Properties, err = loadProperties(filepath.Join(packageDir, "properties.yaml"))
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
	err = yaml.Unmarshal(data, &metadata)
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
	data, err := os.ReadFile(filepath.Join(packageDir, "upgrade-edges.yaml"))
	if err != nil {
		return nil, err
	}
	var ue struct {
		UpgradeEdges UpgradeEdges `json:"upgradeEdges"`
	}
	if err = yaml.Unmarshal(data, &ue); err != nil {
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
	for fromVersion, toVersions := range ue.UpgradeEdges {
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

func loadProperties(propertiesFile string) (Properties, error) {
	data, err := os.ReadFile(propertiesFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var p struct {
		Properties Properties `yaml:"properties"`
	}
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return p.Properties, nil
}

func loadConstraints(constraintsFile string) (Constraints, error) {
	data, err := os.ReadFile(constraintsFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var c struct {
		Constraints Constraints `yaml:"constraints"`
	}
	err = yaml.Unmarshal(data, &c)
	return c.Constraints, err
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
	var bundle Bundle

	metadataAnnotations, err := loadBundleMetadataAnnotations(os.DirFS(bundleDir))
	if err != nil {
		return nil, fmt.Errorf("error loading metadata annotations: %w", err)
	}
	mt, ok := metadataAnnotations[AnnotationKeyBundleContentMediaType]
	if !ok {
		return nil, fmt.Errorf("could not detect bundle content media type")
	}
	bundle.ContentMediaType = mt
	bundle.Content = BundleContent{FS: os.DirFS(bundleDir)}

	bundle.Metadata, bundle.RelatedImages, err = loadBundleMetadataAndRelatedImages(bundle.ContentMediaType, bundleDir, metadataAnnotations)
	if err != nil {
		return nil, fmt.Errorf("error loading metadata: %w", err)
	}
	bundle.Properties, err = loadProperties(filepath.Join(bundleDir, "metadata", "properties.yaml"))
	if err != nil {
		return nil, fmt.Errorf("error loading properties: %w", err)
	}
	bundle.Constraints, err = loadConstraints(filepath.Join(bundleDir, "metadata", "constraints.yaml"))
	if err != nil {
		return nil, fmt.Errorf("error loading constraints: %w", err)
	}

	return &bundle, nil
}

type annotationsFile struct {
	Annotations map[string]string `json:"annotations"`
}

func loadBundleMetadataAnnotations(root fs.FS) (map[string]string, error) {
	if root == nil {
		return nil, fmt.Errorf("filesystem is nil")
	}
	annotationsPath := filepath.Join("metadata", "annotations.yaml")
	annotationsData, err := fs.ReadFile(root, annotationsPath)
	if err != nil {
		return nil, fmt.Errorf("load %s: %v", annotationsPath, err)
	}
	var annotations annotationsFile
	if err := yaml.Unmarshal(annotationsData, &annotations); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %v", annotationsPath, err)
	}

	if pkgName, ok := annotations.Annotations["operators.operatorframework.io.bundle.package.v1"]; ok {
		annotations.Annotations[AnnotationKeyBundlePackage] = pkgName
	}
	if mediatype, ok := annotations.Annotations["operators.operatorframework.io.bundle.mediatype.v1"]; ok {
		annotations.Annotations[AnnotationKeyBundleContentMediaType] = mediatype
	}
	delete(annotations.Annotations, "operators.operatorframework.io.bundle.channel.default.v1")
	delete(annotations.Annotations, "operators.operatorframework.io.bundle.channels.v1")
	delete(annotations.Annotations, "operators.operatorframework.io.bundle.manifests.v1")
	delete(annotations.Annotations, "operators.operatorframework.io.bundle.mediatype.v1")
	delete(annotations.Annotations, "operators.operatorframework.io.bundle.metadata.v1")
	delete(annotations.Annotations, "operators.operatorframework.io.bundle.package.v1")

	return annotations.Annotations, nil
}

func loadBundleMetadataAndRelatedImages(mediaType string, bundleDir string, metadataAnnotations map[string]string) (BundleMetadata, RelatedImages, error) {
	pkgName, ok := metadataAnnotations[AnnotationKeyBundlePackage]
	if !ok {
		return BundleMetadata{}, RelatedImages{}, fmt.Errorf("missing bundle package annotation %q", AnnotationKeyBundlePackage)
	}
	if mediaType == MediaTypeBundleFormatRegistryV1 {
		return loadBundleMetadataAndRelatedImagesRegistryV1(bundleDir, pkgName)
	}

	v, ok := metadataAnnotations[AnnotationKeyBundleVersion]
	if !ok {
		return BundleMetadata{}, RelatedImages{}, fmt.Errorf("missing bundle version annotation %q", AnnotationKeyBundleVersion)
	}
	bundleVersion, err := semver.Parse(v)
	if err != nil {
		return BundleMetadata{}, RelatedImages{}, fmt.Errorf("invalid bundle version %q: %v", v, err)
	}

	var bundleRelease uint64
	r, ok := metadataAnnotations[AnnotationKeyBundleRelease]
	if ok {
		bundleRelease, err = strconv.ParseUint(r, 10, 64)
		if err != nil {
			return BundleMetadata{}, RelatedImages{}, fmt.Errorf("invalid bundle release %q: %v", r, err)
		}
	}

	riData, err := os.ReadFile(filepath.Join(bundleDir, "metadata", "relatedImages.yaml"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return BundleMetadata{}, RelatedImages{}, fmt.Errorf("load related images: %v", err)
	}
	var ri struct {
		RelatedImages RelatedImages `json:"relatedImages"`
	}
	if err := yaml.Unmarshal(riData, &ri); err != nil {
		return BundleMetadata{}, RelatedImages{}, fmt.Errorf("unmarshal related images: %v", err)
	}
	return BundleMetadata{
		Package: pkgName,
		Version: bundleVersion,
		Release: uint(bundleRelease),
	}, ri.RelatedImages, nil
}

func getRegistryBundleRelatedImages(b registry.Bundle) (RelatedImages, error) {
	csv, err := b.ClusterServiceVersion()
	if err != nil {
		return nil, err
	}

	var objmap map[string]*json.RawMessage
	if err = json.Unmarshal(csv.Spec, &objmap); err != nil {
		return nil, err
	}

	var relatedImages RelatedImages
	rawValue, ok := objmap["relatedImages"]
	if ok && rawValue != nil {
		if err = json.Unmarshal(*rawValue, &relatedImages); err != nil {
			return nil, err
		}
	}

	// Keep track of the images we've already found, so that we don't add
	// them multiple times.
	allImages := sets.NewString()
	for _, ri := range relatedImages {
		allImages = allImages.Insert(ri.Image)
	}

	opImages, err := csv.GetOperatorImages()
	if err != nil {
		return nil, err
	}
	for img := range opImages {
		if !allImages.Has(img) {
			relatedImages = append(relatedImages, RelatedImage{
				Image: img,
			})
		}
		allImages = allImages.Insert(img)
	}

	sort.Slice(relatedImages, func(i, j int) bool {
		if relatedImages[i].Image != relatedImages[j].Image {
			return relatedImages[i].Image < relatedImages[j].Image
		}
		return relatedImages[i].Name < relatedImages[j].Name
	})
	return relatedImages, nil
}

func loadBundleMetadataAndRelatedImagesRegistryV1(bundleDir string, pkgName string) (BundleMetadata, RelatedImages, error) {
	logrus.SetOutput(io.Discard)
	ii, err := registry.NewImageInput(image.SimpleReference("placeholder"), bundleDir)
	if err != nil {
		return BundleMetadata{}, RelatedImages{}, fmt.Errorf("error creating image input: %v", err)
	}

	relatedImages, err := getRegistryBundleRelatedImages(*ii.Bundle)
	if err != nil {
		return BundleMetadata{}, RelatedImages{}, fmt.Errorf("error getting related images: %v", err)
	}
	verStr, err := ii.Bundle.Version()
	if err != nil {
		return BundleMetadata{}, RelatedImages{}, fmt.Errorf("error getting bundle version: %v", err)
	}
	version, err := semver.Parse(verStr)
	if err != nil {
		return BundleMetadata{}, RelatedImages{}, fmt.Errorf("invalid bundle version %q: %v", verStr, err)
	}

	return BundleMetadata{
		Package: pkgName,
		Version: version,
	}, relatedImages, nil
}

func LoadChannel(channelDir string, bundles []Bundle) (*Channel, error) {
	var (
		channel Channel
		err     error
	)

	channel.Metadata, err = loadChannelMetadata(filepath.Join(channelDir, "channel.yaml"))
	if err != nil {
		return nil, fmt.Errorf("error loading metadata: %w", err)
	}
	channel.Bundles, err = loadEntries(channelDir, bundles)
	if err != nil {
		return nil, fmt.Errorf("error loading entries: %w", err)
	}
	channel.Properties, err = loadProperties(filepath.Join(channelDir, "properties.yaml"))
	if err != nil {
		return nil, fmt.Errorf("error loading properties: %w", err)
	}

	return &channel, nil
}

func loadEntries(channelDir string, bundles []Bundle) ([]Bundle, error) {
	data, err := os.ReadFile(filepath.Join(channelDir, "entries.yaml"))
	if err != nil {
		return nil, err
	}
	var entries struct {
		Entries []string `json:"entries"`
	}
	if err := yaml.Unmarshal(data, &entries); err != nil {
		return nil, err
	}

	byVersion := map[string][]Bundle{}
	for _, bundle := range bundles {
		byVersion[bundle.Metadata.Version.String()] = append(byVersion[bundle.Metadata.Version.String()], bundle)
	}

	var out []Bundle
	for _, entry := range entries.Entries {
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
	err = yaml.Unmarshal(data, &metadata)
	return metadata, err
}

func (p Package) ArtifactType() string {
	return MediaTypePackage
}

func (p Package) Annotations() map[string]string {
	return map[string]string{AnnotationKeyName: p.Metadata.Name}
}

func (p Package) SubArtifacts() []client.Artifact {
	var artifacts []client.Artifact
	for _, ch := range p.Channels {
		artifacts = append(artifacts, ch)
	}
	return artifacts
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
}

type Maintainer struct {
	Email string `json:"email"`
	Name  string `json:"name,omitempty"`
}

func (pm PackageMetadata) MediaType() string {
	return MediaTypePackageMetadata
}

func (pm PackageMetadata) Data() (io.ReadCloser, error) {
	data, err := yaml.Marshal(pm)
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
	data, err := yaml.Marshal(ue)
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

func (c Channel) SubArtifacts() []client.Artifact {
	var artifacts []client.Artifact
	for _, b := range c.Bundles {
		artifacts = append(artifacts, b)
	}
	return artifacts
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
	data, err := yaml.Marshal(cm)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type Bundle struct {
	Metadata      BundleMetadata
	Properties    Properties
	Constraints   Constraints
	RelatedImages RelatedImages

	ContentMediaType string
	Content          BundleContent

	Digest digest.Digest
}

func (b Bundle) ArtifactType() string {
	return MediaTypeBundle
}

func (b Bundle) Annotations() map[string]string {
	return map[string]string{
		AnnotationKeyBundleVersion:          b.Metadata.Version.String(),
		AnnotationKeyBundleRelease:          fmt.Sprintf("%d", b.Metadata.Release),
		AnnotationKeyBundleContentMediaType: b.ContentMediaType,
	}
}

func (b Bundle) SubArtifacts() []client.Artifact {
	return nil
}

func (b Bundle) Blobs() []client.Blob {
	blobs := []client.Blob{b.Metadata}
	if len(b.Properties) > 0 {
		blobs = append(blobs, b.Properties)
	}
	if len(b.Constraints) > 0 {
		blobs = append(blobs, b.Constraints)
	}
	if len(b.RelatedImages) > 0 {
		blobs = append(blobs, b.RelatedImages)
	}
	blobs = append(blobs, b.Content)
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
	data, err := yaml.Marshal(bm)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type RelatedImage struct {
	Image string `json:"image"`
	Name  string `json:"name,omitempty"`
}

type RelatedImages []RelatedImage

func (ri RelatedImages) MediaType() string {
	return MediaTypeRelatedImages
}

func (ri RelatedImages) Data() (io.ReadCloser, error) {
	data, err := yaml.Marshal(ri)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

type BundleContent struct {
	FS fs.FS
}

func (bc BundleContent) MediaType() string {
	return MediaTypeBundleContent
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
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

func (tv TypeValues) Data() (io.ReadCloser, error) {
	data, err := yaml.Marshal(tv)
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

func (c Catalog) ToFBC(ctx context.Context, repo string) (*declcfg.DeclarativeConfig, error) {
	out := &declcfg.DeclarativeConfig{}
	for _, p := range c.Packages {
		p, err := p.ToFBC(ctx, repo)
		if err != nil {
			return nil, err
		}
		out.Packages = append(out.Packages, p.Packages...)
		out.Channels = append(out.Channels, p.Channels...)
		out.Bundles = append(out.Bundles, p.Bundles...)
		out.Others = append(out.Others, p.Others...)
	}
	return out, nil
}

func (p Package) ToFBC(ctx context.Context, repo string) (*declcfg.DeclarativeConfig, error) {
	pkg := declcfg.Package{
		Schema:      declcfg.SchemaPackage,
		Name:        p.Metadata.Name,
		Description: string(p.Description),
		Properties:  convertTypeValues(p.Properties),
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
			if len(p.UpgradeEdges) == 0 {
				entries = append(entries, declcfg.ChannelEntry{
					Name: bundleName(b),
				})
				continue
			}
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
			Schema:     declcfg.SchemaChannel,
			Package:    p.Metadata.Name,
			Name:       ch.Metadata.Name,
			Entries:    entries,
			Properties: convertTypeValues(ch.Properties),
		})

		for _, b := range ch.Bundles {
			if err := b.ensureDigest(ctx); err != nil {
				return nil, err
			}

			mtValue, err := json.Marshal(b.ContentMediaType)
			if err != nil {
				return nil, fmt.Errorf("error marshalling content media type: %w", err)
			}

			packageProp := map[string]any{
				"packageName": b.Metadata.Package,
				"version":     b.Metadata.Version,
				"release":     b.Metadata.Release,
			}

			pkgPropValue, err := json.Marshal(packageProp)
			if err != nil {
				return nil, fmt.Errorf("error marshalling bundle metadata: %w", err)
			}
			b.Properties = append(b.Properties,
				TypeValue{
					Type:  "olm.bundle.mediatype",
					Value: mtValue,
				},
				TypeValue{
					Type:  "olm.package",
					Value: pkgPropValue,
				},
			)

			bundleMap[fullVersion(b)] = declcfg.Bundle{
				Schema:     declcfg.SchemaBundle,
				Package:    p.Metadata.Name,
				Name:       bundleName(b),
				Image:      fmt.Sprintf("oci://%s@%s", repo, b.Digest),
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

func (b *Bundle) ensureDigest(ctx context.Context) error {
	if b.Content.FS == nil {
		if b.Digest != "" {
			// trust what's already here
			return nil
		}
		return fmt.Errorf("cannot compute digest for sparse bundle")
	}
	st := memory.New()
	desc, err := client.Push(ctx, b, st)
	if err != nil {
		return err
	}
	b.Digest = desc.Digest
	return nil

}
