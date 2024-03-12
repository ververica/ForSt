// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Env methods from Java side.

#include "env/flink/env_flink.h"

#include <jni.h>

#include <vector>

#include "java/rocksjni/portal.h"
#include "rocksdb/env.h"

/*
 * Class:     org_rocksdb_FlinkEnv
 * Method:    createFlinkEnv
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_FlinkEnv_createFlinkEnv(JNIEnv* env, jclass,
                                               jstring j_fs_name) {
  jboolean has_exception = JNI_FALSE;
  auto fs_name =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, j_fs_name, &has_exception);
  if (has_exception == JNI_TRUE) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Could not copy jstring to std::string");
    return 0;
  }
  std::unique_ptr<ROCKSDB_NAMESPACE::Env> flink_env;
  auto status = ROCKSDB_NAMESPACE::NewFlinkEnv(fs_name, &flink_env);
  if (!status.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
    return 0;
  }
  auto ptr_as_handle = flink_env.release();
  return reinterpret_cast<jlong>(ptr_as_handle);
}

/*
 * Class:     org_rocksdb_FlinkEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_FlinkEnv_disposeInternal(JNIEnv*, jobject,
                                               jlong jhandle) {
  auto* handle = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  assert(handle != nullptr);
  delete handle;
}
