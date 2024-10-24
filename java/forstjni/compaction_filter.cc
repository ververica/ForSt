// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::CompactionFilter.

#include "rocksdb/compaction_filter.h"

#include <jni.h>

#include "include/org_forstdb_AbstractCompactionFilter.h"

// <editor-fold desc="org.forstdb.AbstractCompactionFilter">

/*
 * Class:     org_forstdb_AbstractCompactionFilter
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_forstdb_AbstractCompactionFilter_disposeInternal(JNIEnv* /*env*/,
                                                               jobject /*jobj*/,
                                                               jlong handle) {
  auto* cf = reinterpret_cast<ROCKSDB_NAMESPACE::CompactionFilter*>(handle);
  assert(cf != nullptr);
  delete cf;
}
// </editor-fold>
