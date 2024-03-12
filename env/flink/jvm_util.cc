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

#include "env/flink/jvm_util.h"

namespace ROCKSDB_NAMESPACE {

std::atomic<JavaVM*> jvm_ = std::atomic<JavaVM*>(nullptr);

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved) {
  JNIEnv* env = nullptr;
  if (vm->GetEnv((void**)&env, JNI_VERSION_1_8) != JNI_OK) {
    return -1;
  }

  jvm_.store(vm);
  return JNI_VERSION_1_8;
}

JNIEXPORT void JNICALL JNI_OnUnload(JavaVM* vm, void* reserved) {
  jvm_.store(nullptr);
}

void setJVM(JavaVM* jvm) { jvm_.store(jvm); }

JNIEnv* getJNIEnv(bool attach) {
  JavaVM* jvm = jvm_.load();
  if (jvm == nullptr) {
    return nullptr;
  }

  thread_local JavaEnv env;
  if (env.getEnv() == nullptr) {
    auto status = jvm->GetEnv((void**)&(env.getEnv()), JNI_VERSION_1_8);
    if (attach && (status == JNI_EDETACHED || env.getEnv() == nullptr)) {
      if (jvm->AttachCurrentThread((void**)&(env.getEnv()), nullptr) ==
          JNI_OK) {
        env.setNeedDetach();
      }
    }
  }
  return env.getEnv();
}
}  // namespace ROCKSDB_NAMESPACE