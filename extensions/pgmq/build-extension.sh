#!/bin/bash

set -xe

# This script is for running the action .github/actions/build-extension-package locally

GIT_ROOT=$(git rev-parse --show-toplevel)
THIS_DIR=${GIT_ROOT}/extensions/pgmq

# Cleanup files created by this script
function cleanup {
  rm ${THIS_DIR}/Dockerfile
  rm -rf ${THIS_DIR}/docker
  rm ${THIS_DIR}/.dockerignore
}
trap cleanup EXIT

cp ${GIT_ROOT}/.github/actions/build-extension-package/Dockerfile .
cp -R ${GIT_ROOT}/.github/actions/build-extension-package/docker .
echo "target/**" >> .dockerignore

random_tag=extension-build-$(echo $RANDOM)
docker build . --build-arg UBUNTU_VERSION="22.04" \
               --build-arg PGX_VERSION=0.7.1 \
               --build-arg PACKAGE_VERSION=0.0.1 \
               --build-arg NAME=pgmq \
               --build-arg PACKAGE_NAME=pgmq \
               --build-arg PGVERSION=15 \
               -t ${random_tag}
docker run -v $(pwd):/output ${random_tag}
