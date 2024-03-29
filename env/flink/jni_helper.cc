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

#include "jni_helper.h"

#include "jvm_util.h"

namespace ROCKSDB_NAMESPACE {

JavaClassCache::JavaClassCache(JNIEnv* env) : jni_env_(env) {}

JavaClassCache::~JavaClassCache() {
  // Release all global ref of cached jclasses
  for (const auto& item : cached_java_classes_) {
    if (item.javaClass) {
      jni_env_->DeleteGlobalRef(item.javaClass);
    }
  }
}

IOStatus JavaClassCache::Create(JNIEnv* env,
                                std::unique_ptr<JavaClassCache>* result) {
  auto classCache = new JavaClassCache(env);
  IOStatus status = classCache->Init();
  if (!status.ok()) {
    delete classCache;
    result->reset();
    return status;
  }
  result->reset(classCache);
  return status;
}

IOStatus JavaClassCache::Init() {
  // Set all class names
  cached_java_classes_[CachedJavaClass::JC_URI].className = "java/net/URI";
  cached_java_classes_[CachedJavaClass::JC_BYTE_BUFFER].className =
      "java/nio/ByteBuffer";
  cached_java_classes_[CachedJavaClass::JC_THROWABLE].className =
      "java/lang/Throwable";
  cached_java_classes_[CachedJavaClass::JC_FLINK_PATH].className =
      "org/apache/flink/core/fs/Path";
  cached_java_classes_[CachedJavaClass::JC_FLINK_FILE_SYSTEM].className =
      "org/apache/flink/state/forst/fs/ForStFlinkFileSystem";
  cached_java_classes_[CachedJavaClass::JC_FLINK_FILE_STATUS].className =
      "org/apache/flink/core/fs/FileStatus";
  cached_java_classes_[CachedJavaClass::JC_FLINK_FS_INPUT_STREAM].className =
      "org/apache/flink/state/forst/fs/ByteBufferReadableFSDataInputStream";
  cached_java_classes_[CachedJavaClass::JC_FLINK_FS_OUTPUT_STREAM].className =
      "org/apache/flink/state/forst/fs/ByteBufferWritableFSDataOutputStream";

  // Create and set the jclass objects based on the class names set above
  int numCachedClasses =
      sizeof(cached_java_classes_) / sizeof(JavaClassContext);
  for (int i = 0; i < numCachedClasses; i++) {
    IOStatus status = initCachedClass(cached_java_classes_[i].className,
                                      &cached_java_classes_[i].javaClass);
    if (!status.ok()) {
      return status;
    }
  }

  // Set all method names, signatures and class infos
  cached_java_methods_[CachedJavaMethod::JM_FLINK_PATH_CONSTRUCTOR]
      .javaClassAndName = cached_java_classes_[JC_FLINK_PATH];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_PATH_CONSTRUCTOR].methodName =
      "<init>";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_PATH_CONSTRUCTOR].signature =
      "(Ljava/lang/String;)V";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_PATH_TO_STRING]
      .javaClassAndName = cached_java_classes_[JC_FLINK_PATH];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_PATH_TO_STRING].methodName =
      "toString";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_PATH_TO_STRING].signature =
      "()Ljava/lang/String;";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_URI_CONSTRUCTOR]
      .javaClassAndName = cached_java_classes_[JC_URI];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_URI_CONSTRUCTOR].methodName =
      "<init>";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_URI_CONSTRUCTOR].signature =
      "(Ljava/lang/String;)V";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_GET]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_SYSTEM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_GET].methodName =
      "get";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_GET].signature =
      "(Ljava/net/URI;)Lorg/apache/flink/core/fs/FileSystem;";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_GET].isStatic =
      true;

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_EXISTS]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_SYSTEM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_EXISTS]
      .methodName = "exists";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_EXISTS]
      .signature = "(Lorg/apache/flink/core/fs/Path;)Z";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_LIST_STATUS]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_SYSTEM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_LIST_STATUS]
      .methodName = "listStatus";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_LIST_STATUS]
      .signature =
      "(Lorg/apache/flink/core/fs/Path;)[Lorg/apache/flink/core/fs/FileStatus;";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_GET_FILE_STATUS]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_SYSTEM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_GET_FILE_STATUS]
      .methodName = "getFileStatus";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_GET_FILE_STATUS]
      .signature =
      "(Lorg/apache/flink/core/fs/Path;)Lorg/apache/flink/core/fs/FileStatus;";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_DELETE]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_SYSTEM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_DELETE]
      .methodName = "delete";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_DELETE]
      .signature = "(Lorg/apache/flink/core/fs/Path;Z)Z";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_MKDIR]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_SYSTEM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_MKDIR]
      .methodName = "mkdirs";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_MKDIR].signature =
      "(Lorg/apache/flink/core/fs/Path;)Z";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_RENAME_FILE]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_SYSTEM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_RENAME_FILE]
      .methodName = "rename";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_RENAME_FILE]
      .signature =
      "(Lorg/apache/flink/core/fs/Path;Lorg/apache/flink/core/fs/Path;)Z";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_OPEN]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_SYSTEM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_OPEN].methodName =
      "open";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_OPEN].signature =
      "(Lorg/apache/flink/core/fs/Path;)Lorg/apache/flink/state/forst/fs/"
      "ByteBufferReadableFSDataInputStream;";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_INPUT_STREAM_SEQ_READ]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FS_INPUT_STREAM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_INPUT_STREAM_SEQ_READ]
      .methodName = "readFully";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_INPUT_STREAM_SEQ_READ]
      .signature = "(Ljava/nio/ByteBuffer;)I";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_INPUT_STREAM_RANDOM_READ]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FS_INPUT_STREAM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_INPUT_STREAM_RANDOM_READ]
      .methodName = "readFully";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_INPUT_STREAM_RANDOM_READ]
      .signature = "(JLjava/nio/ByteBuffer;)I";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_INPUT_STREAM_SKIP]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FS_INPUT_STREAM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_INPUT_STREAM_SKIP]
      .methodName = "skip";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_INPUT_STREAM_SKIP]
      .signature = "(J)J";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_WRITE]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FS_OUTPUT_STREAM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_WRITE]
      .methodName = "write";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_WRITE]
      .signature = "(Ljava/nio/ByteBuffer;)V";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_FLUSH]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FS_OUTPUT_STREAM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_FLUSH]
      .methodName = "flush";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_FLUSH]
      .signature = "()V";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_SYNC]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FS_OUTPUT_STREAM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_SYNC]
      .methodName = "sync";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_SYNC]
      .signature = "()V";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_CLOSE]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FS_OUTPUT_STREAM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_CLOSE]
      .methodName = "close";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FS_OUTPUT_STREAM_CLOSE]
      .signature = "()V";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_CREATE]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_SYSTEM];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_CREATE]
      .methodName = "create";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_SYSTEM_CREATE]
      .signature =
      "(Lorg/apache/flink/core/fs/Path;)Lorg/apache/flink/state/forst/fs/"
      "ByteBufferWritableFSDataOutputStream;";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_STATUS_GET_PATH]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_STATUS];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_STATUS_GET_PATH]
      .methodName = "getPath";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_STATUS_GET_PATH]
      .signature = "()Lorg/apache/flink/core/fs/Path;";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_STATUS_GET_LEN]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_STATUS];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_STATUS_GET_LEN]
      .methodName = "getLen";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_STATUS_GET_LEN]
      .signature = "()J";

  cached_java_methods_
      [CachedJavaMethod::JM_FLINK_FILE_STATUS_GET_MODIFICATION_TIME]
          .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_STATUS];
  cached_java_methods_
      [CachedJavaMethod::JM_FLINK_FILE_STATUS_GET_MODIFICATION_TIME]
          .methodName = "getModificationTime";
  cached_java_methods_
      [CachedJavaMethod::JM_FLINK_FILE_STATUS_GET_MODIFICATION_TIME]
          .signature = "()J";

  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_STATUS_IS_DIR]
      .javaClassAndName = cached_java_classes_[JC_FLINK_FILE_STATUS];
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_STATUS_IS_DIR]
      .methodName = "isDir";
  cached_java_methods_[CachedJavaMethod::JM_FLINK_FILE_STATUS_IS_DIR]
      .signature = "()Z";

  // Create and set the jmethod based on the method names and signatures set
  // above
  int numCachedMethods =
      sizeof(cached_java_methods_) / sizeof(JavaMethodContext);
  for (int i = 0; i < numCachedMethods; i++) {
    if (cached_java_methods_[i].isStatic) {
      cached_java_methods_[i].javaMethod = jni_env_->GetStaticMethodID(
          cached_java_methods_[i].javaClassAndName.javaClass,
          cached_java_methods_[i].methodName,
          cached_java_methods_[i].signature);
    } else {
      cached_java_methods_[i].javaMethod = jni_env_->GetMethodID(
          cached_java_methods_[i].javaClassAndName.javaClass,
          cached_java_methods_[i].methodName,
          cached_java_methods_[i].signature);
    }

    if (!cached_java_methods_[i].javaMethod) {
      return IOStatus::IOError(std::string("Exception when GetMethodID, ")
                                   .append(cached_java_methods_[i].ToString()));
    }
  }
  return IOStatus::OK();
}

IOStatus JavaClassCache::initCachedClass(const char* className,
                                         jclass* cachedJclass) {
  jclass tempLocalClassRef = jni_env_->FindClass(className);
  if (!tempLocalClassRef) {
    return IOStatus::IOError("Exception when FindClass, class name: " +
                             std::string(className));
  }
  *cachedJclass = (jclass)jni_env_->NewGlobalRef(tempLocalClassRef);
  if (!*cachedJclass) {
    return IOStatus::IOError("Exception when NewGlobalRef, class name " +
                             std::string(className));
  }

  jni_env_->DeleteLocalRef(tempLocalClassRef);
  return IOStatus::OK();
}

JavaClassCache::JavaClassContext JavaClassCache::GetJClass(
    CachedJavaClass cachedJavaClass) {
  return cached_java_classes_[cachedJavaClass];
}

JavaClassCache::JavaMethodContext JavaClassCache::GetJMethod(
    CachedJavaMethod cachedJavaMethod) {
  return cached_java_methods_[cachedJavaMethod];
}

IOStatus JavaClassCache::ConstructPathInstance(const std::string& file_path,
                                               jobject* pathInstance) {
  JNIEnv* jniEnv = getJNIEnv();
  JavaClassCache::JavaClassContext pathClass =
      GetJClass(JavaClassCache::JC_FLINK_PATH);
  JavaClassCache::JavaMethodContext pathConstructor =
      GetJMethod(JavaClassCache::JM_FLINK_PATH_CONSTRUCTOR);
  jstring pathString = jniEnv->NewStringUTF(file_path.c_str());
  jobject tempPathInstance = jniEnv->NewObject(
      pathClass.javaClass, pathConstructor.javaMethod, pathString);
  jniEnv->DeleteLocalRef(pathString);
  if (tempPathInstance == nullptr) {
    return CheckThenError(std::string("Exception when ConstructPathInstance, ")
                              .append(pathClass.ToString())
                              .append(pathConstructor.ToString())
                              .append(", args: Path(")
                              .append(file_path)
                              .append(")"));
  }
  *pathInstance = tempPathInstance;
  return IOStatus::OK();
}

IOStatus CurrentStatus(
    const std::function<std::string()>& exceptionMessageIfError) {
  JNIEnv* jniEnv = getJNIEnv();
  if (jniEnv->ExceptionCheck()) {
    // Throw Exception to Java side, stop any call from Java.
    jthrowable throwable = jniEnv->ExceptionOccurred();
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    jniEnv->Throw(throwable);
    return IOStatus::IOError(exceptionMessageIfError());
  }
  return IOStatus::OK();
}

IOStatus CheckThenError(const std::string& exceptionMessageIfError) {
  JNIEnv* jniEnv = getJNIEnv();
  if (jniEnv->ExceptionCheck()) {
    // Throw Exception to Java side, stop any call from Java.
    jthrowable throwable = jniEnv->ExceptionOccurred();
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    jniEnv->Throw(throwable);
  }
  return IOStatus::IOError(exceptionMessageIfError);
}

}  // namespace ROCKSDB_NAMESPACE