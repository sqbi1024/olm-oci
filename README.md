# Use OCI registries to store OLM artifacts

## What artifacts are related to OLM?

### Catalog

A catalog is a collection of package references that together represent a repository of cluster extensions. 
A catalog can also have metadata associated with it (e.g. publisher, display name, deprecation status).

### Package

A package represents a single product. It contains a collection of channels that help users consume upgrades.
A package author can include multiple channels in their package to give their users different upgrade strategies.
In addition to channels, a package also contains metadata such as icon, description, maintainers, URLs, keywords,
display name, deprecation status, etc.

### Channel

A channel defines a supported upgrade graph that connects versions of the package. A channel lists the bundles that
are present in the channel along with upgrade edge metadata specific to the channel for each of those bundles.
In addition, a channel include metadata such as a description and a deprecation status.

### Bundle

A bundle is an installable unit capable of being deployed on a Kubernetes cluster. It contains content, metadata, and
image references necessary to enable bundle-aware tooling to unpack, deploy, mirror, and provide other package
management utilities.

Bundle content is extensible and can be distinguished based on its media type. Examples of bundle content include OLM's
registry+v1 bundles, rukpak's plain+v0 bundles, and helm charts.

Bundle metadata consists of both properties and constraints, which are useful for cluster package managers to provide 
capabilities around package discovery, dependency/conflict resolution, version pinning, etc.

Lastly a bundle references all container images that a user of the bundle would potentially need to run.

### OCI Index Images & Image Manifests

_(aka manifests lists and container images)_

The typical thing you think of pushing and pulling from image registries. These are generally the container images that
are eventually deployed onto a Kubernetes cluster as a `Pod` directly or via another workload API.

## Why?

Using OCI artifacts to store and share catalogs, packages, channels, and bundles is a natural fit. We think using
cloud-native storage for cloud-native content makes a lot of sense. There are many benefits, but here are a few:

1. Different catalogs can reference the same packages, channels can reference the same bundles, bundles can reference
   the same container images (e.g. sidecar images, operand images, etc.). OCI registries are very good at de-duplicating
   these shared artifacts, which makes them optimal for this use case where lots of sharing occurs.
2. Actions like channel promotion don't involve a complete rebuild of the catalog. You would push a single channel
   artifact update with the new bundle reference, a new package artifact update with the updated channel reference, and
   a new catalog artifact with the updated package reference. Only a very small sliver of the overall catalog would
   change, which means less bandwidth required to push the change and less bandwidth required for users to pull or mirror
   the change.
3. There's no concern about a mismatch between what’s in the catalog and what’s in the bundle. The catalog is simply the
   root node of a directed acyclic graph of artifacts, where any change must be reflected back to the root to have an
   affect to a catalog consumer. Immutability is built-in.
4. Identity is easy. Each artifact has a digest-based OCI artifact reference.
5. Clients don't have to fetch the entire catalog, packages, channels, bundles, and container images. They can instead
   query the registry for _just_ what they need.
6. Using OCI artifact and nesting constructs make mirroring a simple, straightforward process using off the shelf OCI-compliant
   tools.
7. We no longer have to ship binaries around in catalog images that are currently require to serve the content from
   those images. In many cases, those binaries are larger than the catalog itself.

