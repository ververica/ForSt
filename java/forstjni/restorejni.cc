// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling C++ ROCKSDB_NAMESPACE::RestoreOptions methods
// from Java side.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>

#include "include/org_forstdb_RestoreOptions.h"
#include "rocksdb/utilities/backup_engine.h"
#include "forstjni/cplusplus_to_java_convert.h"
#include "forstjni/portal.h"
/*
 * Class:     org_forstdb_RestoreOptions
 * Method:    newRestoreOptions
 * Signature: (Z)J
 */
jlong Java_org_forstdb_RestoreOptions_newRestoreOptions(
    JNIEnv* /*env*/, jclass /*jcls*/, jboolean keep_log_files) {
  auto* ropt = new ROCKSDB_NAMESPACE::RestoreOptions(keep_log_files);
  return GET_CPLUSPLUS_POINTER(ropt);
}

/*
 * Class:     org_forstdb_RestoreOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_forstdb_RestoreOptions_disposeInternal(JNIEnv* /*env*/,
                                                     jobject /*jobj*/,
                                                     jlong jhandle) {
  auto* ropt = reinterpret_cast<ROCKSDB_NAMESPACE::RestoreOptions*>(jhandle);
  assert(ropt);
  delete ropt;
}
