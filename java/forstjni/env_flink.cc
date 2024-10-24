/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "env/flink/env_flink.h"

#include <jni.h>

#include "include/org_forstdb_FlinkEnv.h"

#include "java/forstjni/portal.h"
#include "rocksdb/env.h"

/*
 * Class:     org_forstdb_FlinkEnv
 * Method:    createFlinkEnv
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_forstdb_FlinkEnv_createFlinkEnv(JNIEnv* env, jclass,
                                               jstring base_path) {
  jboolean has_exception = JNI_FALSE;
  auto path =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, base_path, &has_exception);
  if (has_exception == JNI_TRUE) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Could not copy jstring to std::string");
    return 0;
  }
  std::unique_ptr<ROCKSDB_NAMESPACE::Env> flink_env;
  auto status = ROCKSDB_NAMESPACE::NewFlinkEnv(path, &flink_env);
  if (!status.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
    return 0;
  }
  auto ptr_as_handle = flink_env.release();
  return reinterpret_cast<jlong>(ptr_as_handle);
}

/*
 * Class:     org_forstdb_FlinkEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_forstdb_FlinkEnv_disposeInternal(JNIEnv*, jobject,
                                               jlong jhandle) {
  auto* handle = reinterpret_cast<ROCKSDB_NAMESPACE::Env*>(jhandle);
  assert(handle != nullptr);
  delete handle;
}
