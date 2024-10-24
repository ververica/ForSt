// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ ROCKSDB_NAMESPACE::SstFileManager methods
// from Java side.

#include "rocksdb/sst_partitioner.h"

#include <jni.h>

#include <memory>

#include "include/org_forstdb_SstPartitionerFixedPrefixFactory.h"
#include "rocksdb/sst_file_manager.h"
#include "forstjni/cplusplus_to_java_convert.h"
#include "forstjni/portal.h"

/*
 * Class:     org_forstdb_SstPartitionerFixedPrefixFactory
 * Method:    newSstPartitionerFixedPrefixFactory0
 * Signature: (J)J
 */
jlong Java_org_forstdb_SstPartitionerFixedPrefixFactory_newSstPartitionerFixedPrefixFactory0(
    JNIEnv*, jclass, jlong prefix_len) {
  auto* ptr = new std::shared_ptr<ROCKSDB_NAMESPACE::SstPartitionerFactory>(
      ROCKSDB_NAMESPACE::NewSstPartitionerFixedPrefixFactory(prefix_len));
  return GET_CPLUSPLUS_POINTER(ptr);
}

/*
 * Class:     org_forstdb_SstPartitionerFixedPrefixFactory
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_forstdb_SstPartitionerFixedPrefixFactory_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  auto* ptr = reinterpret_cast<
      std::shared_ptr<ROCKSDB_NAMESPACE::SstPartitionerFactory>*>(jhandle);
  delete ptr;  // delete std::shared_ptr
}
