#!/usr/bin/env bash

[[ -n "${CATALOG_REPO}" ]] || { echo "CATALOG_REPO must be set!"; exit 1; }

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && git rev-parse --show-toplevel)"

(cd "${root}" && make)

rm -rf ${root}/samples/catalog* && mkdir ${root}/samples/catalog
for bundle in $(find ${root}/samples/bundles -depth 2); do
	name=$(basename "${bundle}")
	echo "==== Building ${bundle} ===="
	${root}/bin/bundlebuild ${bundle} ${root}/samples/catalog/${name}.oci.tar
	echo ""
done

echo "==== Building catalog ===="
${root}/bin/createcatalog ${root}/samples/catalog ${root}/samples/catalog.oci.tar
echo ""

echo "==== Pushing catalog ===="
(cd ${root}/samples && ../bin/olmoci push archive catalog.oci.tar:catalog ${CATALOG_REPO}:catalog)
