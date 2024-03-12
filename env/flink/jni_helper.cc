// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "jni_helper.h"

namespace ROCKSDB_NAMESPACE {

JavaClassCache::JavaClassCache(JNIEnv *env) : jni_env_(env) {
  // Set all class names
  cached_java_classes_[JavaClassCache::JC_URI].className = "java/net/URI";
  cached_java_classes_[JavaClassCache::JC_BYTE_BUFFER].className =
      "java/nio/ByteBuffer";
  cached_java_classes_[JavaClassCache::JC_THROWABLE].className =
      "java/lang/Throwable";
  cached_java_classes_[JavaClassCache::JC_FLINK_PATH].className =
      "org/apache/flink/core/fs/Path";
  cached_java_classes_[JavaClassCache::JC_FLINK_FILE_SYSTEM].className =
      "org/apache/flink/state/forst/fs/ForStFlinkFileSystem";
  cached_java_classes_[JavaClassCache::JC_FLINK_FILE_STATUS].className =
      "org/apache/flink/core/fs/FileStatus";
  cached_java_classes_[JavaClassCache::JC_FLINK_FS_INPUT_STREAM].className =
      "org/apache/flink/state/forst/fs/ByteBufferReadableFSDataInputStream";
  cached_java_classes_[JavaClassCache::JC_FLINK_FS_OUTPUT_STREAM].className =
      "org/apache/flink/state/forst/fs/ByteBufferWritableFSDataOutputStream";

  // Try best to create and set the jclass objects based on the class names set
  // above
  int numCachedClasses =
      sizeof(cached_java_classes_) / sizeof(javaClassAndName);
  for (int i = 0; i < numCachedClasses; i++) {
    initCachedClass(cached_java_classes_[i].className,
                    &cached_java_classes_[i].javaClass);
  }
}

JavaClassCache::~JavaClassCache() {
  // Release all global ref of cached jclasses
  for (const auto &item : cached_java_classes_) {
    if (item.javaClass) {
      jni_env_->DeleteGlobalRef(item.javaClass);
    }
  }
}

Status JavaClassCache::initCachedClass(const char *className,
                                       jclass *cachedJclass) {
  jclass tempLocalClassRef = jni_env_->FindClass(className);
  if (!tempLocalClassRef) {
    return Status::IOError("Exception when FindClass, class name: " +
                           std::string(className));
  }
  *cachedJclass = (jclass)jni_env_->NewGlobalRef(tempLocalClassRef);
  if (!*cachedJclass) {
    return Status::IOError("Exception when NewGlobalRef, class name " +
                           std::string(className));
  }

  jni_env_->DeleteLocalRef(tempLocalClassRef);
  return Status::OK();
}

Status JavaClassCache::GetJClass(CachedJavaClass cachedJavaClass,
                                 jclass *javaClass) {
  jclass targetClass = cached_java_classes_[cachedJavaClass].javaClass;
  Status status = Status::OK();
  if (!targetClass) {
    status = initCachedClass(cached_java_classes_[cachedJavaClass].className,
                             &targetClass);
  }
  *javaClass = targetClass;
  return status;
}

}  // namespace ROCKSDB_NAMESPACE