#!/bin/bash
# This script is a hack because, apparently, we can't set arguments in FROM
# if not dealing with the first stage
version=$1
filename=$2
out=$3

sed "s/@@PG_VERSION@@/$version/g" $filename > $out
