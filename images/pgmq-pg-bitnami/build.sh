#!/bin/bash

args=("$@")

deb_release=12

if [[ " ${args[@]} " =~ [[:blank:]]+--debian-release=([[:digit:]]+)[[:blank:]]+ ]]
then
	deb_release=${BASH_REMATCH[1]}
fi

pg_version=17

if [[ " ${args[@]} " =~ [[:blank:]]+--postgresql-version=([[:digit:]]+)[[:blank:]]+ ]]
then
	pg_version=${BASH_REMATCH[1]}
fi

tag="tembo/pgmq:pg${pg_version}-deb${deb_release}-v$(git rev-parse --short HEAD)"

if [[ " ${args[@]} " =~ [[:blank:]]+--tag=([[:alnum:]]+)[[:blank:]]+ ]]
then
	tag=${BASH_REMATCH[1]}
fi

(
	cd $(git rev-parse --show-toplevel)

	docker build \
	--build-arg DEB_RELEASE=${deb_release} \
	--build-arg PG_VERSION=${pg_version} \
	-t "${tag}" \
	-f images/pgmq-pg-bitnami/Dockerfile.in .
)
