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

#include <functional>
#include <vector>

#include "jni.h"
#include "rocksdb/io_status.h"

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

  // Frequently-used method type representing jmethods which will be cached.
  typedef enum {
    JM_FLINK_PATH_CONSTRUCTOR,
    JM_FLINK_PATH_TO_STRING,
    JM_FLINK_URI_CONSTRUCTOR,
    JM_FLINK_FILE_SYSTEM_GET,
    JM_FLINK_FILE_SYSTEM_EXISTS,
    JM_FLINK_FILE_SYSTEM_LIST_STATUS,
    JM_FLINK_FILE_SYSTEM_GET_FILE_STATUS,
    JM_FLINK_FILE_SYSTEM_DELETE,
    JM_FLINK_FILE_SYSTEM_MKDIR,
    JM_FLINK_FILE_SYSTEM_RENAME_FILE,
    JM_FLINK_FILE_SYSTEM_OPEN,
    JM_FLINK_FS_INPUT_STREAM_SEQ_READ,
    JM_FLINK_FS_INPUT_STREAM_RANDOM_READ,
    JM_FLINK_FS_INPUT_STREAM_SKIP,
    JM_FLINK_FS_OUTPUT_STREAM_WRITE,
    JM_FLINK_FS_OUTPUT_STREAM_FLUSH,
    JM_FLINK_FS_OUTPUT_STREAM_SYNC,
    JM_FLINK_FS_OUTPUT_STREAM_CLOSE,
    JM_FLINK_FILE_SYSTEM_CREATE,
    JM_FLINK_FILE_STATUS_GET_PATH,
    JM_FLINK_FILE_STATUS_GET_LEN,
    JM_FLINK_FILE_STATUS_GET_MODIFICATION_TIME,
    JM_FLINK_FILE_STATUS_IS_DIR,
    NUM_CACHED_METHODS
  } CachedJavaMethod;

  // jclass with its context description
  struct JavaClassContext {
    jclass javaClass;
    const char* className;

    std::string ToString() const {
      return std::string("className: ").append(className);
    }
  };

  // jmethod with its context description
  struct JavaMethodContext {
    JavaClassContext javaClassAndName;
    jmethodID javaMethod;
    const char* methodName;
    const char* signature;
    bool isStatic = false;

    std::string ToString() const {
      return javaClassAndName.ToString()
          .append(", methodName: ")
          .append(methodName)
          .append(", signature: ")
          .append(signature)
          .append(", isStatic:")
          .append(isStatic ? "true" : "false");
    }
  };

  ~JavaClassCache();

  // Create a unique instance which inits necessary cached classes and methods.
  // Return Status representing whether these classes and methods are inited
  // correctly or not.
  static IOStatus Create(JNIEnv* env,
                         std::unique_ptr<JavaClassCache>* javaClassCache);

  // Get JavaClassContext by specific CachedJavaClass.
  JavaClassContext GetJClass(CachedJavaClass cachedJavaClass);

  // Get JavaMethodContext by specific CachedJavaMethod.
  JavaMethodContext GetJMethod(CachedJavaMethod cachedJavaMethod);

  // Construct Java Path Instance based on cached classes and method related to
  // Path.
  IOStatus ConstructPathInstance(const std::string& /*file_path*/,
                                 jobject* /*pathInstance*/);

 private:
  JNIEnv* jni_env_;
  JavaClassContext cached_java_classes_[CachedJavaClass::NUM_CACHED_CLASSES];
  JavaMethodContext cached_java_methods_[CachedJavaMethod::NUM_CACHED_METHODS];

  explicit JavaClassCache(JNIEnv* env);

  // Init all classes and methods.
  IOStatus Init();

  // Init cached class.
  IOStatus initCachedClass(const char* className, jclass* cachedClass);
};

// Return current status of JNIEnv.
IOStatus CurrentStatus(
    const std::function<std::string()>& /*exceptionMessageIfError*/);

// Wrap error status of JNIEnv.
IOStatus CheckThenError(const std::string& /*exceptionMessageIfError*/);

}  // namespace ROCKSDB_NAMESPACE