#!/usr/bin/env bash
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

set -e
#set -x

# just in-case this is run outside Docker
mkdir -p /rocksdb-local-build

rm -rf /rocksdb-local-build/*
cp -r /rocksdb-host/* /rocksdb-local-build
cd /rocksdb-local-build

make clean-not-downloaded
PORTABLE=1 make -j2 rocksdbjavastatic

cp java/target/libforstjni-linux*.so java/target/forstjni-*-linux*.jar java/target/forstjni-*-linux*.jar.sha1 /rocksdb-java-target
