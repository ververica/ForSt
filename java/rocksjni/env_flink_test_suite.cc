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

#include "env/flink/env_flink_test_suite.h"

#include <jni.h>

#include "include/org_rocksdb_EnvFlinkTestSuite.h"
#include "java/rocksjni/portal.h"

/*
 * Class:     org_rocksdb_EnvFlinkTestSuite
 * Method:    buildNativeObject
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_EnvFlinkTestSuite_buildNativeObject(JNIEnv* env, jobject,
                                                           jstring basePath) {
  jboolean has_exception = JNI_FALSE;
  auto path =
      ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, basePath, &has_exception);
  if (has_exception == JNI_TRUE) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(
        env, "Could not copy jstring to std::string");
    return 0;
  }
  auto env_flink_test_suites = new ROCKSDB_NAMESPACE::EnvFlinkTestSuites(path);
  return reinterpret_cast<jlong>(env_flink_test_suites);
}

/*
 * Class:     org_rocksdb_EnvFlinkTestSuite
 * Method:    runAllTestSuites
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_EnvFlinkTestSuite_runAllTestSuites(
    JNIEnv* jniEnv, jobject, jlong objectHandle) {
  auto env_flink_test_suites =
      reinterpret_cast<ROCKSDB_NAMESPACE::EnvFlinkTestSuites*>(objectHandle);
  env_flink_test_suites->runAllTestSuites();
  if (jniEnv->ExceptionCheck()) {
    jthrowable throwable = jniEnv->ExceptionOccurred();
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    jniEnv->Throw(throwable);
  }
}

/*
 * Class:     org_rocksdb_EnvFlinkTestSuite
 * Method:    disposeInternal
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_EnvFlinkTestSuite_disposeInternal(
    JNIEnv*, jobject, jlong objectHandle) {
  auto test_suites =
      reinterpret_cast<ROCKSDB_NAMESPACE::EnvFlinkTestSuites*>(objectHandle);
  delete test_suites;
}