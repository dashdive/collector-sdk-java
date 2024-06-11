#!/bin/bash

pushd "$(dirname "$0")" > /dev/null

mkdir -p compat_patches
rm -f compat_patches/*.patch

COMPAT_BRANCHES=$(git branch | cat | grep -E '^\s+compat__' | awk '{print $1}')
for branch in $COMPAT_BRANCHES; do
    patch_name=${branch:8}
    git diff main $branch > "compat_patches/$patch_name.patch"
    echo "Generated patch: compat_patches/$patch_name.patch"
done

popd > /dev/null
