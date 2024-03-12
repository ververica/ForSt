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

#pragma once

#include <atomic>
#include <cstdbool>
#include <cstddef>
#include <string>

#include "jni.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

extern std::atomic<JavaVM*> jvm_;

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved);
JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* reserved);

#ifdef __cplusplus
}
#endif

void setJVM(JavaVM* jvm);

JNIEnv* getJNIEnv(bool attach = true);

static inline std::string parseJavaString(JNIEnv* jni_env,
                                          jstring java_string) {
  const char* chars = jni_env->GetStringUTFChars(java_string, nullptr);
  auto length = jni_env->GetStringUTFLength(java_string);
  std::string native_string = std::string(chars, length);
  jni_env->ReleaseStringUTFChars(java_string, chars);
  return native_string;
}

class JavaEnv {
 public:
  ~JavaEnv() {
    if (env_ != nullptr && need_detach_) {
      jvm_.load()->DetachCurrentThread();
      need_detach_ = false;
    }
  }

  JNIEnv*& getEnv() { return env_; }

  void setNeedDetach() { need_detach_ = true; }

 private:
  JNIEnv* env_ = nullptr;
  bool need_detach_ = false;
};
}  // namespace ROCKSDB_NAMESPACE