// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::Comparator.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <functional>
#include <string>

#include "include/org_forstdb_AbstractComparator.h"
#include "include/org_forstdb_NativeComparatorWrapper.h"
#include "forstjni/comparatorjnicallback.h"
#include "forstjni/cplusplus_to_java_convert.h"
#include "forstjni/portal.h"

/*
 * Class:     org_forstdb_AbstractComparator
 * Method:    createNewComparator
 * Signature: (J)J
 */
jlong Java_org_forstdb_AbstractComparator_createNewComparator(
    JNIEnv* env, jobject jcomparator, jlong copt_handle) {
  auto* copt =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallbackOptions*>(
          copt_handle);
  auto* c =
      new ROCKSDB_NAMESPACE::ComparatorJniCallback(env, jcomparator, copt);
  return GET_CPLUSPLUS_POINTER(c);
}

/*
 * Class:     org_forstdb_AbstractComparator
 * Method:    usingDirectBuffers
 * Signature: (J)Z
 */
jboolean Java_org_forstdb_AbstractComparator_usingDirectBuffers(JNIEnv*,
                                                                jobject,
                                                                jlong jhandle) {
  auto* c =
      reinterpret_cast<ROCKSDB_NAMESPACE::ComparatorJniCallback*>(jhandle);
  return static_cast<jboolean>(c->m_options->direct_buffer);
}

/*
 * Class:     org_forstdb_NativeComparatorWrapper
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_forstdb_NativeComparatorWrapper_disposeInternal(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jcomparator_handle) {
  auto* comparator =
      reinterpret_cast<ROCKSDB_NAMESPACE::Comparator*>(jcomparator_handle);
  delete comparator;
}
