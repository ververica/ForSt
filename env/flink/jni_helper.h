// Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "jni.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// A cache for java classes to avoid calling FindClass frequently
class JavaClassCache {
 public:
  // Frequently-used class type representing jclasses which will be cached.
  typedef enum {
    JC_URI,
    JC_BYTE_BUFFER,
    JC_THROWABLE,
    JC_FLINK_PATH,
    JC_FLINK_FILE_SYSTEM,
    JC_FLINK_FILE_STATUS,
    JC_FLINK_FS_INPUT_STREAM,
    JC_FLINK_FS_OUTPUT_STREAM,
    NUM_CACHED_CLASSES
  } CachedJavaClass;

  // Constructor and Destructor
  explicit JavaClassCache(JNIEnv* env);
  ~JavaClassCache();

  // Get jclass by specific CachedJavaClass
  Status GetJClass(CachedJavaClass cachedJavaClass, jclass* javaClass);

 private:
  typedef struct {
    jclass javaClass;
    const char* className;
  } javaClassAndName;

  JNIEnv* jni_env_;
  javaClassAndName cached_java_classes_[JavaClassCache::NUM_CACHED_CLASSES];

  Status initCachedClass(const char* className, jclass* cachedClass);
};
}  // namespace ROCKSDB_NAMESPACE