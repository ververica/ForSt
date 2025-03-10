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

#include "env_flink.h"

#include "jvm_util.h"
#include "logging/env_logger.h"

//
// This file defines a Flink environment for ForSt. It uses the JNI call
// to access Flink FileSystem. All files created by one instance of ForSt
// will reside on the actual Flink FileSystem.
//
namespace ROCKSDB_NAMESPACE {

// Appends to an existing file in Flink FileSystem.
class FlinkWritableFile : public FSWritableFile {
 private:
  const std::string file_path_;
  const jobject file_system_instance_;
  jobject fs_data_output_stream_instance_;
  JavaClassCache* class_cache_;
  bool closed_;

 public:
  FlinkWritableFile(jobject file_system_instance,
                    JavaClassCache* java_class_cache,
                    const std::string& file_path, const FileOptions& options)
      : FSWritableFile(options),
        file_path_(file_path),
        file_system_instance_(file_system_instance),
        fs_data_output_stream_instance_(nullptr),
        class_cache_(java_class_cache),
        closed_(false) {}

  ~FlinkWritableFile() override {
    JNIEnv* jniEnv = getJNIEnv();
    if (fs_data_output_stream_instance_ != nullptr) {
      InnerClose();
      jniEnv->DeleteGlobalRef(fs_data_output_stream_instance_);
    }
  }

  IOStatus Init() {
    JNIEnv* jniEnv = getJNIEnv();
    jstring pathString = jniEnv->NewStringUTF(file_path_.c_str());

    JavaClassCache::JavaMethodContext fileSystemCreateMethod =
        class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_SYSTEM_CREATE);
    jobject fsDataOutputStream = jniEnv->CallObjectMethod(
        file_system_instance_, fileSystemCreateMethod.javaMethod, pathString);
    jniEnv->DeleteLocalRef(pathString);
    if (fsDataOutputStream == nullptr || jniEnv->ExceptionCheck()) {
      return CheckThenError(
          std::string(
              "CallObjectMethod Exception when Init FlinkWritableFile, ")
              .append(fileSystemCreateMethod.ToString())
              .append(", args: Path(")
              .append(file_path_)
              .append(")"));
    }
    fs_data_output_stream_instance_ = jniEnv->NewGlobalRef(fsDataOutputStream);
    jniEnv->DeleteLocalRef(fsDataOutputStream);
    return IOStatus::OK();
  }

  IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
    JNIEnv* jniEnv = getJNIEnv();
    if (data.size() > static_cast<size_t>(LONG_MAX)) {
      return IOStatus::IOError(
          std::string("Append too big data to file, data: ")
              .append(data.ToString()));
    }
    jobject directByteBuffer = jniEnv->NewDirectByteBuffer(
        (void*)data.data(), static_cast<long>(data.size()));

    JavaClassCache::JavaMethodContext writeMethod = class_cache_->GetJMethod(
        JavaClassCache::JM_FLINK_FS_OUTPUT_STREAM_WRITE);
    jniEnv->CallVoidMethod(fs_data_output_stream_instance_,
                           writeMethod.javaMethod, directByteBuffer);
    jniEnv->DeleteLocalRef(directByteBuffer);

    std::string filePath = file_path_;
    return CurrentStatus([filePath]() {
      return std::string("Exception when Appending file, path: ")
          .append(filePath);
    });
  }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& /* verification_info */,
                  IODebugContext* dbg) override {
    return Append(data, options, dbg);
  }

  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    JavaClassCache::JavaMethodContext flushMethod = class_cache_->GetJMethod(
        JavaClassCache::JM_FLINK_FS_OUTPUT_STREAM_FLUSH);
    JNIEnv* jniEnv = getJNIEnv();
    jniEnv->CallVoidMethod(fs_data_output_stream_instance_,
                           flushMethod.javaMethod);

    std::string filePath = file_path_;
    return CurrentStatus([filePath]() {
      return std::string("Exception when Flush file, path: ").append(filePath);
    });
  }

  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override {
    JavaClassCache::JavaMethodContext flushMethod = class_cache_->GetJMethod(
        JavaClassCache::JM_FLINK_FS_OUTPUT_STREAM_SYNC);
    JNIEnv* jniEnv = getJNIEnv();
    jniEnv->CallVoidMethod(fs_data_output_stream_instance_,
                           flushMethod.javaMethod);

    std::string filePath = file_path_;
    return CurrentStatus([filePath]() {
      return std::string("Exception when Sync file, path: ").append(filePath);
    });
  }

  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return InnerClose();
  }

  IOStatus InnerClose() {
    if (closed_) {
      return IOStatus::OK();
    }
    closed_ = true;
    JavaClassCache::JavaMethodContext closeMethod = class_cache_->GetJMethod(
        JavaClassCache::JM_FLINK_FS_OUTPUT_STREAM_CLOSE);
    JNIEnv* jniEnv = getJNIEnv();
    jniEnv->CallVoidMethod(fs_data_output_stream_instance_,
                           closeMethod.javaMethod);

    std::string filePath = file_path_;
    return CurrentStatus([filePath]() {
      return std::string("Exception when Close file, path: ").append(filePath);
    });
  }
};

// Used for reading a file from Flink FileSystem. It implements both
// sequential-read access methods and random read access methods.
class FlinkReadableFile : virtual public FSSequentialFile,
                          virtual public FSRandomAccessFile {
 private:
  const std::string file_path_;
  const jobject file_system_instance_;
  jobject fs_data_input_stream_instance_;
  JavaClassCache* class_cache_;

 public:
  FlinkReadableFile(jobject file_system_instance,
                    JavaClassCache* java_class_cache,
                    const std::string& file_path)
      : file_path_(file_path),
        file_system_instance_(file_system_instance),
        fs_data_input_stream_instance_(nullptr),
        class_cache_(java_class_cache) {}

  ~FlinkReadableFile() override {
    JNIEnv* jniEnv = getJNIEnv();
    if (fs_data_input_stream_instance_ != nullptr) {
      JavaClassCache::JavaMethodContext closeMethod = class_cache_->GetJMethod(
          JavaClassCache::JM_FLINK_FS_INPUT_STREAM_CLOSE);
      jniEnv->CallVoidMethod(fs_data_input_stream_instance_,
                             closeMethod.javaMethod);
      jniEnv->DeleteGlobalRef(fs_data_input_stream_instance_);
    }
  }

  IOStatus Init() {
    JNIEnv* jniEnv = getJNIEnv();
    jstring pathString = jniEnv->NewStringUTF(file_path_.c_str());

    JavaClassCache::JavaMethodContext openMethod =
        class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_SYSTEM_OPEN);
    jobject fsDataInputStream = jniEnv->CallObjectMethod(
        file_system_instance_, openMethod.javaMethod, pathString);
    jniEnv->DeleteLocalRef(pathString);
    if (fsDataInputStream == nullptr || jniEnv->ExceptionCheck()) {
      return CheckThenError(
          std::string(
              "CallObjectMethod Exception when Init FlinkReadableFile, ")
              .append(openMethod.ToString())
              .append(", args: Path(")
              .append(file_path_)
              .append(")"));
    }

    fs_data_input_stream_instance_ = jniEnv->NewGlobalRef(fsDataInputStream);
    jniEnv->DeleteLocalRef(fsDataInputStream);
    return IOStatus::OK();
  }

  // sequential access, read data at current offset in file
  IOStatus Read(size_t n, const IOOptions& /*options*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
    JNIEnv* jniEnv = getJNIEnv();
    if (n > static_cast<size_t>(LONG_MAX)) {
      return IOStatus::IOError(
          std::string("Read too big data to file, data size: ")
              .append(std::to_string(n)));
    }
    jobject directByteBuffer =
        jniEnv->NewDirectByteBuffer((void*)scratch, static_cast<long>(n));

    JavaClassCache::JavaMethodContext readMethod = class_cache_->GetJMethod(
        JavaClassCache::JM_FLINK_FS_INPUT_STREAM_SEQ_READ);
    jint totalBytesRead =
        jniEnv->CallIntMethod(fs_data_input_stream_instance_,
                              readMethod.javaMethod, directByteBuffer);

    jniEnv->DeleteLocalRef(directByteBuffer);

    std::string filePath = file_path_;
    IOStatus status = CurrentStatus([filePath]() {
      return std::string("Exception when Reading file, path: ")
          .append(filePath);
    });
    if (!status.ok()) {
      return status;
    }

    *result = Slice(scratch, totalBytesRead == -1 ? 0 : totalBytesRead);
    return IOStatus::OK();
  }

  // random access, read data from specified offset in file
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
    JNIEnv* jniEnv = getJNIEnv();
    if (n > static_cast<size_t>(LONG_MAX)) {
      return IOStatus::IOError(
          std::string("Read too big data to file, data size: ")
              .append(std::to_string(n)));
    }
    jobject directByteBuffer =
        jniEnv->NewDirectByteBuffer((void*)scratch, static_cast<long>(n));

    JavaClassCache::JavaMethodContext readMethod = class_cache_->GetJMethod(
        JavaClassCache::JM_FLINK_FS_INPUT_STREAM_RANDOM_READ);
    jint totalBytesRead =
        jniEnv->CallIntMethod(fs_data_input_stream_instance_,
                              readMethod.javaMethod, offset, directByteBuffer);

    jniEnv->DeleteLocalRef(directByteBuffer);

    std::string filePath = file_path_;
    IOStatus status = CurrentStatus([filePath]() {
      return std::string("Exception when Reading file, path: ")
          .append(filePath);
    });
    if (!status.ok()) {
      return status;
    }

    *result = Slice(scratch, totalBytesRead == -1 ? 0 : totalBytesRead);
    return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
    JNIEnv* jniEnv = getJNIEnv();
    JavaClassCache::JavaMethodContext skipMethod =
        class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FS_INPUT_STREAM_SKIP);
    jniEnv->CallVoidMethod(fs_data_input_stream_instance_,
                           skipMethod.javaMethod, n);

    std::string filePath = file_path_;
    return CurrentStatus([filePath]() {
      return std::string("Exception when skipping file, path: ")
          .append(filePath);
    });
  }
};

// Simple implementation of FSDirectory, Shouldn't influence the normal usage
class FlinkDirectory : public FSDirectory {
 public:
  explicit FlinkDirectory() = default;
  ~FlinkDirectory() override = default;

  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    // TODO: Syncing directory is managed by specific flink file system
    // currently, consider to implement in the future
    return IOStatus::OK();
  }
};

FlinkFileSystem::FlinkFileSystem(const std::shared_ptr<FileSystem>& base_fs,
                                 const std::string& base_path,
                                 jobject file_system_instance)
    : FileSystemWrapper(base_fs), base_path_(TrimTrailingSlash(base_path)) {
  if (file_system_instance != nullptr) {
    JNIEnv* env = getJNIEnv();
    file_system_instance_ = env->NewGlobalRef(file_system_instance);
  } else {
    file_system_instance_ = nullptr;
  }
}

FlinkFileSystem::~FlinkFileSystem() {
  if (file_system_instance_ != nullptr) {
    JNIEnv* env = getJNIEnv();
    env->DeleteGlobalRef(file_system_instance_);
  }
  delete class_cache_;
}

Status FlinkFileSystem::Init() {
  JNIEnv* jniEnv = getJNIEnv();
  std::unique_ptr<JavaClassCache> javaClassCache;
  Status status = JavaClassCache::Create(jniEnv, &javaClassCache);
  if (!status.ok()) {
    return status;
  }
  class_cache_ = javaClassCache.release();

  if (file_system_instance_ == nullptr) {
    // Delegate Flink to load real FileSystem (e.g.
    // S3FileSystem/OSSFileSystem/...)
    JavaClassCache::JavaClassContext fileSystemClass =
        class_cache_->GetJClass(JavaClassCache::JC_FLINK_FILE_SYSTEM);
    JavaClassCache::JavaMethodContext fileSystemGetMethod =
        class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_SYSTEM_GET);

    jstring uriStringArg = jniEnv->NewStringUTF(base_path_.c_str());

    // Construct FileSystem
    jobject fileSystemInstance = jniEnv->CallStaticObjectMethod(
        fileSystemClass.javaClass, fileSystemGetMethod.javaMethod,
        uriStringArg);
    jniEnv->DeleteLocalRef(uriStringArg);
    if (fileSystemInstance == nullptr || jniEnv->ExceptionCheck()) {
      return CheckThenError(
          std::string(
              "CallStaticObjectMethod Exception when Init FlinkFileSystem, ")
              .append(fileSystemClass.ToString())
              .append(fileSystemGetMethod.ToString())
              .append(", args: URI(")
              .append(base_path_)
              .append(")"));
    }
    file_system_instance_ = jniEnv->NewGlobalRef(fileSystemInstance);
    jniEnv->DeleteLocalRef(fileSystemInstance);
  }

  if (file_system_instance_ == nullptr) {
    return CheckThenError(std::string(
        "Error when init flink env, the file system provided is null"));
  }

  if (jniEnv->ExceptionCheck()) {
    return CheckThenError(
        std::string("Error when init flink env, JNI throws exception."));
  }
  return Status::OK();
}

std::string FlinkFileSystem::ConstructPath(const std::string& fname) {
  return fname.at(0) == '/' ? base_path_ + fname : base_path_ + "/" + fname;
}

// open a file for sequential reading
IOStatus FlinkFileSystem::NewSequentialFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSSequentialFile>* result, IODebugContext* dbg) {
  result->reset();
  IOStatus status = FileExists(fname, options.io_options, dbg);
  if (!status.ok()) {
    return status;
  }

  auto f = new FlinkReadableFile(file_system_instance_, class_cache_,
                                 ConstructPath(fname));
  IOStatus valid = f->Init();
  if (!valid.ok()) {
    delete f;
    return valid;
  }
  result->reset(f);
  return IOStatus::OK();
}

// open a file for random reading
IOStatus FlinkFileSystem::NewRandomAccessFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSRandomAccessFile>* result, IODebugContext* dbg) {
  result->reset();
  IOStatus status = FileExists(fname, options.io_options, dbg);
  if (!status.ok()) {
    return status;
  }

  auto f = new FlinkReadableFile(file_system_instance_, class_cache_,
                                 ConstructPath(fname));
  IOStatus valid = f->Init();
  if (!valid.ok()) {
    delete f;
    return valid;
  }
  result->reset(f);
  return IOStatus::OK();
}

// create a new file for writing
IOStatus FlinkFileSystem::NewWritableFile(
    const std::string& fname, const FileOptions& options,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* /*dbg*/) {
  result->reset();
  auto f = new FlinkWritableFile(file_system_instance_, class_cache_,
                                 ConstructPath(fname), options);
  IOStatus valid = f->Init();
  if (!valid.ok()) {
    delete f;
    return valid;
  }
  result->reset(f);
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::NewDirectory(const std::string& name,
                                       const IOOptions& options,
                                       std::unique_ptr<FSDirectory>* result,
                                       IODebugContext* dbg) {
  result->reset();
  IOStatus s = FileExists(name, options, dbg);
  if (s.ok()) {
    result->reset(new FlinkDirectory());
  }
  return s;
}

IOStatus FlinkFileSystem::FileExists(const std::string& file_name,
                                     const IOOptions& /*options*/,
                                     IODebugContext* /*dbg*/) {
  std::string filePath = ConstructPath(file_name);
  JNIEnv* jniEnv = getJNIEnv();
  jstring pathString = jniEnv->NewStringUTF(filePath.c_str());

  // Call exist method
  JavaClassCache::JavaMethodContext existsMethod =
      class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_SYSTEM_EXISTS);
  jboolean exists = jniEnv->CallBooleanMethod(
      file_system_instance_, existsMethod.javaMethod, pathString);
  jniEnv->DeleteLocalRef(pathString);

  IOStatus status = CurrentStatus([filePath]() {
    return std::string("Exception when FileExists, path: ").append(filePath);
  });
  if (!status.ok()) {
    return status;
  }

  return exists == JNI_TRUE ? IOStatus::OK() : IOStatus::NotFound();
}

// TODO: Not Efficient! Consider adding usable methods in FLink FileSystem
IOStatus FlinkFileSystem::GetChildren(const std::string& file_name,
                                      const IOOptions& options,
                                      std::vector<std::string>* result,
                                      IODebugContext* dbg) {
  IOStatus fileExistsStatus = FileExists(file_name, options, dbg);
  if (!fileExistsStatus.ok()) {
    return fileExistsStatus.IsNotFound()
               ? IOStatus::PathNotFound(
                     std::string("Could not find path when GetChildren, path: ")
                         .append(ConstructPath(file_name)))
               : fileExistsStatus;
  }

  std::string filePath = ConstructPath(file_name);
  JNIEnv* jniEnv = getJNIEnv();
  jstring pathString = jniEnv->NewStringUTF(filePath.c_str());

  JavaClassCache::JavaMethodContext listStatusMethod = class_cache_->GetJMethod(
      JavaClassCache::JM_FLINK_FILE_SYSTEM_LIST_STATUS);

  auto fileStatusArray = (jobjectArray)jniEnv->CallObjectMethod(
      file_system_instance_, listStatusMethod.javaMethod, pathString);
  jniEnv->DeleteLocalRef(pathString);
  if (fileStatusArray == nullptr || jniEnv->ExceptionCheck()) {
    return CheckThenError(
        std::string("Exception when CallObjectMethod in GetChildren, ")
            .append(listStatusMethod.ToString())
            .append(", args: Path(")
            .append(filePath)
            .append(")"));
  }

  jsize fileStatusArrayLen = jniEnv->GetArrayLength(fileStatusArray);
  for (jsize i = 0; i < fileStatusArrayLen; i++) {
    jobject fileStatusObj = jniEnv->GetObjectArrayElement(fileStatusArray, i);
    if (fileStatusObj == nullptr || jniEnv->ExceptionCheck()) {
      jniEnv->DeleteLocalRef(fileStatusArray);
      return CheckThenError(
          "Exception when GetObjectArrayElement in GetChildren");
    }

    JavaClassCache::JavaMethodContext getPathMethod =
        class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_STATUS_GET_PATH);
    auto subPathStr = (jstring)jniEnv->CallObjectMethod(
        fileStatusObj, getPathMethod.javaMethod);
    jniEnv->DeleteLocalRef(fileStatusObj);
    if (subPathStr == nullptr || jniEnv->ExceptionCheck()) {
      jniEnv->DeleteLocalRef(fileStatusArray);
      return CheckThenError(
          std::string("Exception when CallObjectMethod in GetChildren, ")
              .append(getPathMethod.ToString()));
    }

    const char* str = jniEnv->GetStringUTFChars(subPathStr, nullptr);
    result->emplace_back(str);
    jniEnv->ReleaseStringUTFChars(subPathStr, str);
    jniEnv->DeleteLocalRef(subPathStr);
  }

  jniEnv->DeleteLocalRef(fileStatusArray);
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::DeleteDir(const std::string& file_name,
                                    const IOOptions& options,
                                    IODebugContext* dbg) {
  return Delete(file_name, options, dbg, true);
};

IOStatus FlinkFileSystem::DeleteFile(const std::string& file_name,
                                     const IOOptions& options,
                                     IODebugContext* dbg) {
  return Delete(file_name, options, dbg, false);
}

IOStatus FlinkFileSystem::Delete(const std::string& file_name,
                                 const IOOptions& options, IODebugContext* dbg,
                                 bool recursive) {
  IOStatus fileExistsStatus = FileExists(file_name, options, dbg);
  if (!fileExistsStatus.ok()) {
    return fileExistsStatus.IsNotFound()
               ? IOStatus::PathNotFound(
                     std::string("Could not find path when Delete, path: ")
                         .append(ConstructPath(file_name)))
               : fileExistsStatus;
  }
  JNIEnv* jniEnv = getJNIEnv();

  std::string filePath = ConstructPath(file_name);
  jstring pathString = jniEnv->NewStringUTF(filePath.c_str());

  // Call delete method
  JavaClassCache::JavaMethodContext deleteMethod =
      class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_SYSTEM_DELETE);
  jboolean deleted = jniEnv->CallBooleanMethod(
      file_system_instance_, deleteMethod.javaMethod, pathString, recursive);
  jniEnv->DeleteLocalRef(pathString);

  IOStatus status = CurrentStatus([filePath]() {
    return std::string("Exception when Delete, path: ").append(filePath);
  });
  if (!status.ok()) {
    return status;
  }

  return deleted
             ? IOStatus::OK()
             : IOStatus::IOError(std::string("Exception when Delete, path: ")
                                     .append(filePath));
}

IOStatus FlinkFileSystem::CreateDir(const std::string& file_name,
                                    const IOOptions& options,
                                    IODebugContext* dbg) {
  IOStatus s = FileExists(file_name, options, dbg);
  if (!s.ok()) {
    return CreateDirIfMissing(file_name, options, dbg);
  }
  return IOStatus::IOError(std::string("Exception when CreateDir because Dir (")
                               .append(file_name)
                               .append(") exists"));
}

IOStatus FlinkFileSystem::CreateDirIfMissing(const std::string& file_name,
                                             const IOOptions& /*options*/,
                                             IODebugContext* /*dbg*/) {
  JNIEnv* jniEnv = getJNIEnv();

  std::string filePath = ConstructPath(file_name);
  jstring pathString = jniEnv->NewStringUTF(filePath.c_str());

  // Call mkdirs method
  JavaClassCache::JavaMethodContext mkdirMethod =
      class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_SYSTEM_MKDIR);
  jboolean created = jniEnv->CallBooleanMethod(
      file_system_instance_, mkdirMethod.javaMethod, pathString);
  jniEnv->DeleteLocalRef(pathString);
  IOStatus status = CurrentStatus([filePath]() {
    return std::string("Exception when CreateDirIfMissing, path: ")
        .append(filePath);
  });
  if (!status.ok()) {
    return status;
  }

  return created ? IOStatus::OK()
                 : IOStatus::IOError(
                       std::string("Exception when CreateDirIfMissing, path: ")
                           .append(filePath));
}

IOStatus FlinkFileSystem::GetFileSize(const std::string& file_name,
                                      const IOOptions& options, uint64_t* size,
                                      IODebugContext* dbg) {
  JNIEnv* jniEnv = getJNIEnv();
  jobject fileStatus;
  IOStatus status = GetFileStatus(file_name, options, dbg, &fileStatus);
  if (!status.ok()) {
    return status;
  }

  JavaClassCache::JavaMethodContext getLenMethod =
      class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_STATUS_GET_LEN);
  jlong fileSize = jniEnv->CallLongMethod(fileStatus, getLenMethod.javaMethod);
  jniEnv->DeleteLocalRef(fileStatus);

  status = CurrentStatus([file_name]() {
    return std::string("Exception when GetFileSize, file name: ")
        .append(file_name);
  });
  if (!status.ok()) {
    return status;
  }

  *size = fileSize;
  return IOStatus::OK();
}

// The life cycle of fileStatus is maintained by caller.
IOStatus FlinkFileSystem::GetFileStatus(const std::string& file_name,
                                        const IOOptions& options,
                                        IODebugContext* dbg,
                                        jobject* fileStatus) {
  IOStatus status = FileExists(file_name, options, dbg);
  if (!status.ok()) {
    return status.IsNotFound()
               ? IOStatus::PathNotFound(
                     std::string(
                         "Could not find path when GetFileStatus, path: ")
                         .append(ConstructPath(file_name)))
               : status;
  }

  JNIEnv* jniEnv = getJNIEnv();

  std::string filePath = ConstructPath(file_name);
  jstring pathString = jniEnv->NewStringUTF(filePath.c_str());

  // Call getFileStatus method
  JavaClassCache::JavaMethodContext getFileStatusMethod =
      class_cache_->GetJMethod(
          JavaClassCache::JM_FLINK_FILE_SYSTEM_GET_FILE_STATUS);
  *fileStatus = jniEnv->CallObjectMethod(
      file_system_instance_, getFileStatusMethod.javaMethod, pathString);
  jniEnv->DeleteLocalRef(pathString);

  return CurrentStatus([filePath]() {
    return std::string("Exception when GetFileStatus, path: ").append(filePath);
  });
}

IOStatus FlinkFileSystem::GetFileModificationTime(const std::string& file_name,
                                                  const IOOptions& options,
                                                  uint64_t* time,
                                                  IODebugContext* dbg) {
  JNIEnv* jniEnv = getJNIEnv();
  jobject fileStatus;
  IOStatus status = GetFileStatus(file_name, options, dbg, &fileStatus);
  if (!status.ok()) {
    return status;
  }

  JavaClassCache::JavaMethodContext getModificationTimeMethod =
      class_cache_->GetJMethod(
          JavaClassCache::JM_FLINK_FILE_STATUS_GET_MODIFICATION_TIME);
  jlong fileModificationTime =
      jniEnv->CallLongMethod(fileStatus, getModificationTimeMethod.javaMethod);
  jniEnv->DeleteLocalRef(fileStatus);

  status = CurrentStatus([file_name]() {
    return std::string("Exception when GetFileModificationTime, file name: ")
        .append(file_name);
  });
  if (!status.ok()) {
    return status;
  }

  *time = fileModificationTime;
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::IsDirectory(const std::string& path,
                                      const IOOptions& options, bool* is_dir,
                                      IODebugContext* dbg) {
  JNIEnv* jniEnv = getJNIEnv();
  jobject fileStatus;
  IOStatus status = GetFileStatus(path, options, dbg, &fileStatus);
  if (!status.ok()) {
    return status;
  }

  JavaClassCache::JavaMethodContext isDirMethod =
      class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_STATUS_IS_DIR);
  jboolean isDir =
      jniEnv->CallBooleanMethod(fileStatus, isDirMethod.javaMethod);
  jniEnv->DeleteLocalRef(fileStatus);

  status = CurrentStatus([path]() {
    return std::string("Exception when IsDirectory, file name: ").append(path);
  });
  if (!status.ok()) {
    return status;
  }

  *is_dir = isDir;
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::RenameFile(const std::string& src,
                                     const std::string& target,
                                     const IOOptions& options,
                                     IODebugContext* dbg) {
  IOStatus status = FileExists(src, options, dbg);
  if (!status.ok()) {
    return status.IsNotFound()
               ? IOStatus::PathNotFound(
                     std::string(
                         "Could not find src path when RenameFile, path: ")
                         .append(ConstructPath(src)))
               : status;
  }

  JNIEnv* jniEnv = getJNIEnv();

  std::string srcFilePath = ConstructPath(src);
  jstring srcPathString = jniEnv->NewStringUTF(srcFilePath.c_str());
  std::string targetFilePath = ConstructPath(target);
  jstring targetPathString = jniEnv->NewStringUTF(targetFilePath.c_str());

  JavaClassCache::JavaMethodContext renameMethod = class_cache_->GetJMethod(
      JavaClassCache::JM_FLINK_FILE_SYSTEM_RENAME_FILE);
  jboolean renamed =
      jniEnv->CallBooleanMethod(file_system_instance_, renameMethod.javaMethod,
                                srcPathString, targetPathString);
  jniEnv->DeleteLocalRef(srcPathString);
  jniEnv->DeleteLocalRef(targetPathString);

  status = CurrentStatus([srcFilePath, targetFilePath]() {
    return std::string("Exception when RenameFile, src: ")
        .append(srcFilePath)
        .append(", target: ")
        .append(targetFilePath);
  });
  if (!status.ok()) {
    return status;
  }

  return renamed
             ? IOStatus::OK()
             : IOStatus::IOError(std::string("Exception when RenameFile, src: ")
                                     .append(srcFilePath)
                                     .append(", target: ")
                                     .append(targetFilePath));
}

IOStatus FlinkFileSystem::LockFile(const std::string& /*file_name*/,
                                   const IOOptions& /*options*/,
                                   FileLock** lock, IODebugContext* /*dbg*/) {
  // There isn't a very good way to atomically check and create a file,
  // Since it will not influence the usage of Flink, just leave it OK() now;
  *lock = nullptr;
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::UnlockFile(FileLock* /*lock*/,
                                     const IOOptions& /*options*/,
                                     IODebugContext* /*dbg*/) {
  // There isn't a very good way to atomically check and create a file,
  // Since it will not influence the usage of Flink, just leave it OK() now;
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::LinkFile(const std::string& src,
                                   const std::string& target,
                                   const IOOptions& options,
                                   IODebugContext* dbg) {
  IOStatus status = FileExists(src, options, dbg);
  if (!status.ok()) {
    return status.IsNotFound()
               ? IOStatus::PathNotFound(
                     std::string(
                         "Could not find src path when linkFile, path: ")
                         .append(ConstructPath(src)))
               : status;
  }

  JNIEnv* jniEnv = getJNIEnv();

  std::string srcFilePath = ConstructPath(src);
  jstring srcPathString = jniEnv->NewStringUTF(srcFilePath.c_str());

  std::string targetFilePath = ConstructPath(target);
  jstring targetPathString = jniEnv->NewStringUTF(targetFilePath.c_str());

  JavaClassCache::JavaMethodContext linkMethod =
      class_cache_->GetJMethod(JavaClassCache::JM_FLINK_FILE_SYSTEM_LINK_FILE);
  jint linked =
      jniEnv->CallIntMethod(file_system_instance_, linkMethod.javaMethod,
                            srcPathString, targetPathString);
  jniEnv->DeleteLocalRef(srcPathString);
  jniEnv->DeleteLocalRef(targetPathString);

  status = CurrentStatus([srcFilePath, targetFilePath]() {
    return std::string("Exception when LinkFile, src: ")
        .append(srcFilePath)
        .append(", target: ")
        .append(targetFilePath);
  });
  if (!status.ok()) {
    return status;
  }

  if (linked == -1) {
    return IOStatus::NotSupported();
  } else if (linked > 0) {
    return IOStatus::IOError(std::string("Exception when LinkFile, src: ")
                                 .append(srcFilePath)
                                 .append(", target: ")
                                 .append(targetFilePath));
  }
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::NewLogger(const std::string& fname,
                                    const IOOptions& io_opts,
                                    std::shared_ptr<Logger>* result,
                                    IODebugContext* dbg) {
  FileOptions options;
  options.io_options = io_opts;
  // TODO: Tune the buffer size.
  options.writable_file_max_buffer_size = 1024 * 1024;
  std::unique_ptr<FSWritableFile> writable_file;
  const IOStatus status = NewWritableFile(fname, options, &writable_file, dbg);
  if (!status.ok()) {
    return status;
  }

  *result = std::make_shared<EnvLogger>(std::move(writable_file), fname,
                                        options, Env::Default());
  return IOStatus::OK();
}

Status FlinkFileSystem::Create(const std::shared_ptr<FileSystem>& base,
                               const std::string& uri,
                               std::unique_ptr<FileSystem>* result,
                               jobject file_system_instance) {
  auto* fileSystem = new FlinkFileSystem(base, uri, file_system_instance);
  Status status = fileSystem->Init();
  result->reset(fileSystem);
  return status;
}

Status NewFlinkEnv(const std::string& uri,
                   std::unique_ptr<Env>* flinkFileSystem,
                   jobject file_system_instance) {
  std::shared_ptr<FileSystem> fs;
  Status s = NewFlinkFileSystem(uri, &fs, file_system_instance);
  if (s.ok()) {
    *flinkFileSystem = NewCompositeEnv(fs);
  }
  return s;
}

Status NewFlinkFileSystem(const std::string& uri,
                          std::shared_ptr<FileSystem>* fs,
                          jobject file_system_instance) {
  std::unique_ptr<FileSystem> flinkFileSystem;
  Status s = FlinkFileSystem::Create(FileSystem::Default(), uri,
                                     &flinkFileSystem, file_system_instance);
  if (s.ok()) {
    fs->reset(flinkFileSystem.release());
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
