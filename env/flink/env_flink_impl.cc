//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <rocksdb/env.h>
#include <algorithm>
#include <iostream>
#include <string>
#include "rocksdb/status.h"
#include "util/string_util.h"
#include "JvmUtils.h"
#include "env_flink.h"

//
// This file defines an HDFS environment for rocksdb. It uses the libhdfs
// api to access HDFS. All HDFS files created by one instance of rocksdb
// will reside on the same HDFS cluster.
//
namespace ROCKSDB_NAMESPACE {

// Appends to an existing file in Flink FileSystem.
class FlinkWritableFile: public FSWritableFile {
 private:
  std::string filename_;
  jclass fs_data_output_stream_class_;
  jobject fs_data_output_stream_instance_;

 public:
  FlinkWritableFile(
      jobject fileSystemInstance, jmethodID createMethodId, jclass cachedPathClass, jmethodID cachedPathConstructor,
      const std::string& fname, const FileOptions& options)
      : FSWritableFile(options),
        filename_(fname) {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    jstring pathString = jniEnv->NewStringUTF(fname.c_str());
    jobject pathInstance = jniEnv->NewObject(cachedPathClass, cachedPathConstructor, pathString);
    if (pathInstance == nullptr) {
      std::cerr << "Could not create instance of class Path" << std::endl;
      return;
    }

    jobject fsDataOutputStream = jniEnv->CallObjectMethod(fileSystemInstance, createMethodId, pathInstance);
    if (fsDataOutputStream == nullptr) {
      std::cerr << "Fail to call FileSystem#create(Path f)" << std::endl;
      return;
    }
    fs_data_output_stream_instance_ = jniEnv->NewGlobalRef(fsDataOutputStream);

    jclass fsDataOutputStreamClass = jniEnv->FindClass("org/apache/flink/state/remote/rocksdb/fs/ByteBufferWritableFSDataOutputStream");
    if (fsDataOutputStreamClass == nullptr) {
      std::cerr << "Fail to find class org/apache/flink/state/remote/rocksdb/fs/ByteBufferWritableFSDataOutputStream" << std::endl;
      return;
    }
    fs_data_output_stream_class_ = (jclass)jniEnv->NewGlobalRef(fsDataOutputStreamClass);

    jniEnv->DeleteLocalRef(fsDataOutputStream);
    jniEnv->DeleteLocalRef(pathInstance);
    jniEnv->DeleteLocalRef(pathString);
  }

  ~FlinkWritableFile() override {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    if (fs_data_output_stream_class_ != nullptr) {
      jniEnv->DeleteGlobalRef(fs_data_output_stream_class_);
    }
    if (fs_data_output_stream_instance_ != nullptr) {
      jniEnv->DeleteGlobalRef(fs_data_output_stream_instance_);
    }
  }

  IOStatus isValid() {
    return fs_data_output_stream_class_ == nullptr ||
                   fs_data_output_stream_instance_ == nullptr ?
               IOStatus::IOError("Fail to create FSDataOutputStream") : IOStatus::OK();
  }

  IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    jmethodID writeMethodId = jniEnv->GetMethodID(fs_data_output_stream_class_, "write", "(Ljava/nio/ByteBuffer;)V");
    if (writeMethodId == nullptr) {
      return IOStatus::IOError("Could not find method ByteBufferWritableFSDataOutputStream#write");
    }
    // TODO: check about unsigned long to long
    jobject directByteBuffer = jniEnv->NewDirectByteBuffer((void*)data.data(), data.size());
    jniEnv->CallVoidMethod(fs_data_output_stream_instance_, writeMethodId, directByteBuffer);

    if (jniEnv->ExceptionCheck()) {
      jniEnv->ExceptionDescribe();
      jniEnv->ExceptionClear();
      return IOStatus::IOError("Exception when Appending file, filename: " + filename_);
    }
    jniEnv->DeleteLocalRef(directByteBuffer);
    return IOStatus::OK();
  }

  IOStatus Append(const Slice& data, const IOOptions& options,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg) override {
    return Append(data, options, dbg);
  }

  IOStatus Flush(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    jmethodID flushMethodId = jniEnv->GetMethodID(fs_data_output_stream_class_, "flush", "()V");
    jniEnv->CallVoidMethod(fs_data_output_stream_instance_, flushMethodId);

    if (jniEnv->ExceptionCheck()) {
      jniEnv->ExceptionDescribe();
      jniEnv->ExceptionClear();
      return IOStatus::IOError("Exception when Flush file, filename: " + filename_);
    }
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions& /*options*/,
                IODebugContext* /*dbg*/) override {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    jmethodID syncMethodId = jniEnv->GetMethodID(fs_data_output_stream_class_, "sync", "()V");
    jniEnv->CallVoidMethod(fs_data_output_stream_instance_, syncMethodId);

    if (jniEnv->ExceptionCheck()) {
      jniEnv->ExceptionDescribe();
      jniEnv->ExceptionClear();
      return IOStatus::IOError("Exception when Sync file, filename: " + filename_);
    }

    return IOStatus::OK();
  }


  IOStatus Close(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    jmethodID closeMethodId = jniEnv->GetMethodID(fs_data_output_stream_class_, "close", "()V");
    jniEnv->CallVoidMethod(fs_data_output_stream_instance_, closeMethodId);

    if (jniEnv->ExceptionCheck()) {
      jniEnv->ExceptionDescribe();
      jniEnv->ExceptionClear();
      return IOStatus::IOError("Exception when Close file, filename: " + filename_);
    }
    return IOStatus::OK();
  }
};

// Used for reading a file from Flink FileSystem. It implements both sequential-read
// access methods and random read access methods.
class FlinkReadableFile : virtual public FSSequentialFile,
                         virtual public FSRandomAccessFile {
 private:
  std::string filename_;
  jclass fs_data_input_stream_class_;
  jobject fs_data_input_stream_instance_;

 public:
  FlinkReadableFile(
      jobject fileSystemInstance, jmethodID openMethodId, jclass cachedPathClass,
      jmethodID cachedPathConstructor, const std::string& fname)
      : filename_(fname) {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    jstring pathString = jniEnv->NewStringUTF(fname.c_str());
    jobject pathInstance = jniEnv->NewObject(cachedPathClass, cachedPathConstructor, pathString);
    if (pathInstance == nullptr) {
      std::cerr << "Could not create instance of class Path" << std::endl;
      return;
    }

    jobject fsDataInputStream = jniEnv->CallObjectMethod(fileSystemInstance, openMethodId, pathInstance);
    if (fsDataInputStream == nullptr) {
      std::cerr << "Fail to call FileSystem#open(Path f)" << std::endl;
      return;
    }
    fs_data_input_stream_instance_ = jniEnv->NewGlobalRef(fsDataInputStream);

    jclass fsDataInputStreamClass = jniEnv->FindClass("org/apache/flink/state/remote/rocksdb/fs/ByteBufferReadableFSDataInputStream");
    if (fsDataInputStreamClass == nullptr) {
      std::cerr << "Fail to find class org/apache/flink/state/remote/rocksdb/fs/ByteBufferReadableFSDataInputStream" << std::endl;
      return;
    }
    fs_data_input_stream_class_ = (jclass)jniEnv->NewGlobalRef(fsDataInputStreamClass);

    jniEnv->DeleteLocalRef(fsDataInputStream);
    jniEnv->DeleteLocalRef(pathInstance);
    jniEnv->DeleteLocalRef(pathString);
  }

  ~FlinkReadableFile() override {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    if (fs_data_input_stream_class_ != nullptr) {
      jniEnv->DeleteGlobalRef(fs_data_input_stream_class_);
    }
    if (fs_data_input_stream_instance_ != nullptr) {
      jniEnv->DeleteGlobalRef(fs_data_input_stream_instance_);
    }
  }

  IOStatus isValid() {
        return fs_data_input_stream_class_ == nullptr ||
                   fs_data_input_stream_instance_ == nullptr ?
                   IOStatus::IOError("Fail to create FSDataInputStream") : IOStatus::OK();
  }

  // sequential access, read data at current offset in file
  IOStatus Read(size_t n, const IOOptions& /*options*/, Slice* result,
                char* scratch, IODebugContext* /*dbg*/) override {
     JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
     jmethodID readMethodId = jniEnv->GetMethodID(fs_data_input_stream_class_, "read", "(Ljava/nio/ByteBuffer;)I");
     if (readMethodId == nullptr) {
        return IOStatus::IOError("Could not find method ByteBufferReadableFSDataInputStream#read");
     }
     // TODO: check about unsigned long to long
     jobject directByteBuffer = jniEnv->NewDirectByteBuffer((void*)scratch, n);
     jint totalBytesRead = jniEnv->CallIntMethod(fs_data_input_stream_instance_, readMethodId, directByteBuffer);
     if (jniEnv->ExceptionCheck()) {
        jniEnv->ExceptionDescribe();
        jniEnv->ExceptionClear();
        return IOStatus::IOError("Exception when Reading file, filename: " + filename_);
     }
     jniEnv->DeleteLocalRef(directByteBuffer);
     *result = Slice(scratch, totalBytesRead == -1 ? 0 : totalBytesRead);
     return IOStatus::OK();
  }

  // random access, read data from specified offset in file
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& /*options*/,
                Slice* result, char* scratch,
                IODebugContext* /*dbg*/) const override {
     JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
     jmethodID readMethodId = jniEnv->GetMethodID(fs_data_input_stream_class_, "read", "(JLjava/nio/ByteBuffer;)I");
     if (readMethodId == nullptr) {
        return IOStatus::IOError("Could not find method ByteBufferReadableFSDataInputStream#read");
     }
     // TODO: check about unsigned long to long
     jobject directByteBuffer = jniEnv->NewDirectByteBuffer((void*)scratch, n);
     jint totalBytesRead = jniEnv->CallIntMethod(fs_data_input_stream_instance_, readMethodId, offset, directByteBuffer);
     if (jniEnv->ExceptionCheck()) {
        jniEnv->ExceptionDescribe();
        jniEnv->ExceptionClear();
        return IOStatus::IOError("Exception when reading file, filename: " + filename_);
     }
     jniEnv->DeleteLocalRef(directByteBuffer);
     *result = Slice(scratch, totalBytesRead == -1 ? 0 : totalBytesRead);
     return IOStatus::OK();
  }

  IOStatus Skip(uint64_t n) override {
     JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
     jmethodID skipMethodId = jniEnv->GetMethodID(fs_data_input_stream_class_, "skip", "(J)J");
     jniEnv->CallVoidMethod(fs_data_input_stream_instance_, skipMethodId, n);
     if (jniEnv->ExceptionCheck()) {
        jniEnv->ExceptionDescribe();
        jniEnv->ExceptionClear();
        return IOStatus::IOError("Exception when skipping file, filename: " + filename_);
     }
     return IOStatus::OK();
  }
};

// Simple implementation of FSDirectory, Shouldn't influence the normal usage
class FlinkDirectory : public FSDirectory {
 public:
  explicit FlinkDirectory(int fd) : fd_(fd) {}
  ~FlinkDirectory() {}

  IOStatus Fsync(const IOOptions& /*options*/,
                 IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  int GetFd() const { return fd_; }

 private:
  int fd_;
};

// TODO: 1. Release all jobjects when exception
// TODO: 2. Cache some classes and methods
// Finally, the FlinkFileSystem
FlinkFileSystem::FlinkFileSystem(const std::shared_ptr<FileSystem>& base, const std::string& fsname)
  : FileSystemWrapper(base), fsname_(fsname) {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();

    // Use Flink FileSystem.get(URI uri) to load real FileSystem (e.g. S3FileSystem/OSSFileSystem/...)
    jclass abstract_file_system_class_ = jniEnv->FindClass("org/apache/flink/state/remote/rocksdb/fs/RemoteRocksdbFlinkFileSystem");
    if (abstract_file_system_class_ == nullptr) {
      std::cerr << "Cannot find abstract_file_system_class_" << std::endl;
      return;
    }
    jmethodID methodId = jniEnv->GetStaticMethodID(
        abstract_file_system_class_, "get", "(Ljava/net/URI;)Lorg/apache/flink/core/fs/FileSystem;");
    if (methodId == nullptr) {
      std::cerr << "Method get Not Found" << std::endl;
      return;
    }
    jclass uriClass = jniEnv->FindClass("java/net/URI");
    if (uriClass == nullptr) {
      std::cerr << "Cannot find uriClass" << std::endl;
      return;
    }
    jmethodID uriCtor = jniEnv->GetMethodID(uriClass, "<init>", "(Ljava/lang/String;)V");
    if (uriCtor == nullptr) {
      std::cerr << "URI Constructor Not Found" << std::endl;
      return;
    }
    jstring uriStringArg = jniEnv->NewStringUTF(fsname.c_str());
    jobject uriInstance = jniEnv->NewObject(uriClass, uriCtor, uriStringArg);
    if (uriInstance == nullptr) {
      std::cerr << "Could not create instance of class URI" << std::endl;
      return;
    }

    jobject fileSystemInstance = jniEnv->CallStaticObjectMethod(file_system_class_, methodId, uriInstance);
    if (fileSystemInstance == nullptr) {
      std::cerr << "Could not call static method get" << std::endl;
      return;
    }
    jclass fileSystemClass = jniEnv->GetObjectClass(fileSystemInstance);
    if (fileSystemClass == nullptr) {
      std::cerr << "Could not GetObjectClass of file_system_instance_" << std::endl;
      return;
    }
    file_system_instance_ = jniEnv->NewGlobalRef(fileSystemInstance);
    file_system_class_ = (jclass)jniEnv->NewGlobalRef(fileSystemClass);

    jniEnv->DeleteLocalRef(uriStringArg);
    jniEnv->DeleteLocalRef(uriInstance);
    jniEnv->DeleteLocalRef(uriClass);
    jniEnv->DeleteLocalRef(abstract_file_system_class_);
    jniEnv->DeleteLocalRef(fileSystemInstance);
    jniEnv->DeleteLocalRef(fileSystemClass);

    // Construct some frequently-used class and method to reduce the cost of JNI
    jclass pathClass = jniEnv->FindClass("org/apache/flink/core/fs/Path");
    if (pathClass == nullptr) {
      std::cerr << "Class org/apache/flink/core/fs/Path not found!" << std::endl;
      return;
    }
    cached_path_class_ = (jclass)jniEnv->NewGlobalRef(pathClass);
    jniEnv->DeleteLocalRef(pathClass);

    cached_path_constructor_ = jniEnv->GetMethodID(cached_path_class_, "<init>", "(Ljava/lang/String;)V");
    if (cached_path_constructor_ == nullptr) {
      std::cerr << "No constructor found for class Path" << std::endl;
      return;
    }
    jclass fileStatusClass = jniEnv->FindClass("org/apache/flink/core/fs/FileStatus");
    if (fileStatusClass == nullptr) {
      std::cerr << "Class org/apache/flink/core/fs/FileStatus not found!" << std::endl;
      return;
    }
    cached_file_status_class_ = (jclass)jniEnv->NewGlobalRef(fileStatusClass);
    jniEnv->DeleteLocalRef(fileStatusClass);
}
  
FlinkFileSystem::~FlinkFileSystem() {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    if (file_system_instance_ != nullptr) {
      jniEnv->DeleteGlobalRef(file_system_instance_);
    }
    if (file_system_class_ != nullptr) {
      jniEnv->DeleteGlobalRef(file_system_class_);
    }
    if (cached_path_class_ != nullptr) {
      jniEnv->DeleteGlobalRef(cached_path_class_);
    }
    if (cached_file_status_class_ != nullptr) {
      jniEnv->DeleteGlobalRef(cached_file_status_class_);
    }
}
  
std::string FlinkFileSystem::GetId() const {
  if (fsname_.empty()) {
    return kProto;
  } else if (StartsWith(fsname_, kProto)) {
    return fsname_;
  } else {
    std::string id = kProto;
    return id.append("default:0").append(fsname_);
  }
}

Status FlinkFileSystem::status() {
  if (file_system_class_ == nullptr || file_system_instance_ == nullptr
      || cached_path_class_ == nullptr || cached_path_constructor_ == nullptr) {
    return Status::IOError();
  }
  return Status::OK();
}

Status FlinkFileSystem::ValidateOptions(const DBOptions& db_opts,
                                       const ColumnFamilyOptions& cf_opts) const {
    return Status::OK();
}
  
// open a file for sequential reading
IOStatus FlinkFileSystem::NewSequentialFile(const std::string& fname,
                                               const FileOptions& options,
                                               std::unique_ptr<FSSequentialFile>* result,
                                               IODebugContext* dbg) {
    result->reset();
    IOStatus status = FileExists(fname, IOOptions(), dbg);
    if (!status.ok()) {
      return status;
    }
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    jmethodID openMethodId = jniEnv->GetMethodID(
        file_system_class_, "open",
        "(Lorg/apache/flink/core/fs/Path;)Lorg/apache/flink/state/remote/rocksdb/fs/ByteBufferReadableFSDataInputStream;");
    if (openMethodId == nullptr) {
      return IOStatus::IOError("Method RemoteRocksdbFlinkFileSystem#open(Path f) not found!");
    }
    auto f = new FlinkReadableFile(
        file_system_instance_, openMethodId,
        cached_path_class_, cached_path_constructor_, fname);
    IOStatus valid = f->isValid();
    if (!valid.ok()) {
      delete f;
      return valid;
    }
    result->reset(f);
    return IOStatus::OK();
}

// open a file for random reading
IOStatus FlinkFileSystem::NewRandomAccessFile(const std::string& fname,
                                                 const FileOptions& options,
                                                 std::unique_ptr<FSRandomAccessFile>* result,
                                                 IODebugContext* dbg) {
    result->reset();
    IOStatus status = FileExists(fname, IOOptions(), dbg);
    if (!status.ok()) {
      return status;
    }
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    jmethodID openMethodId = jniEnv->GetMethodID(
        file_system_class_, "open",
        "(Lorg/apache/flink/core/fs/Path;)Lorg/apache/flink/state/remote/rocksdb/fs/ByteBufferReadableFSDataInputStream;");
    if (openMethodId == nullptr) {
      return IOStatus::IOError("Method RemoteRocksdbFlinkFileSystem#open(Path f) not found!");
    }
    auto f = new FlinkReadableFile(
        file_system_instance_, openMethodId,
        cached_path_class_, cached_path_constructor_, fname);
    IOStatus valid = f->isValid();
    if (!valid.ok()) {
      delete f;
      return valid;
    }
    result->reset(f);
    return IOStatus::OK();
}

// create a new file for writing
IOStatus FlinkFileSystem::NewWritableFile(const std::string& fname,
                                             const FileOptions& options,
                                             std::unique_ptr<FSWritableFile>* result,
                                             IODebugContext* /*dbg*/) {
  result->reset();
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jmethodID createMethodId = jniEnv->GetMethodID(
      file_system_class_, "create",
      "(Lorg/apache/flink/core/fs/Path;)Lorg/apache/flink/state/remote/rocksdb/fs/ByteBufferWritableFSDataOutputStream;");
  if (createMethodId == nullptr) {
    return IOStatus::IOError("Method RemoteRocksdbFlinkFileSystem#create(Path f) not found!");
  }
  auto f = new FlinkWritableFile(
      file_system_instance_, createMethodId,
      cached_path_class_, cached_path_constructor_, fname, options);
  IOStatus valid = f->isValid();
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
  IOStatus s = FileExists(name, options, dbg);
  if (s.ok()) {
    result->reset(new FlinkDirectory(0));
  }
  return s;
}

IOStatus FlinkFileSystem::FileExists(const std::string& fname,
                                        const IOOptions& /*options*/,
                                        IODebugContext* /*dbg*/) {
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jmethodID existsMethodId = jniEnv->GetMethodID(
      file_system_class_, "exists", "(Lorg/apache/flink/core/fs/Path;)Z");
  if (existsMethodId == nullptr) {
    return IOStatus::IOError("Method exists not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(fname.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    return IOStatus::IOError("Could not create instance of class Path");
  }

  jboolean exists = jniEnv->CallBooleanMethod(
      file_system_instance_, existsMethodId, pathInstance);
  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
  }

  jniEnv->DeleteLocalRef(pathString);
  jniEnv->DeleteLocalRef(pathInstance);
  return exists == JNI_TRUE ? IOStatus::OK() : IOStatus::NotFound();
}

// TODO: Not Efficient! Should cache some class and method or add more usable methods in FLink FileSystem
IOStatus FlinkFileSystem::GetChildren(const std::string& path,
                                         const IOOptions& options,
                                         std::vector<std::string>* result, 
                                        IODebugContext* dbg) {
  if (FileExists(path, options, dbg).IsNotFound()){
    return IOStatus::PathNotFound("Could not find path: " + path);
  }
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jmethodID listStatusMethodId = jniEnv->GetMethodID(
      file_system_class_, "listStatus", "(Lorg/apache/flink/core/fs/Path;)[Lorg/apache/flink/core/fs/FileStatus;");
  if (listStatusMethodId == nullptr) {
    return IOStatus::IOError("Method listStatusMethodId not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(path.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    return IOStatus::IOError("Could not create instance of class Path, path: " + path);
  }

  auto fileStatusArray = (jobjectArray)jniEnv->CallObjectMethod(
      file_system_instance_, listStatusMethodId, pathInstance);
  if (fileStatusArray == nullptr) {
    return IOStatus::IOError("Exception when FileSystem.listStatus(Path f), path: " + path);
  }

  jsize fileStatusArrayLen = jniEnv->GetArrayLength(fileStatusArray);
  for (jsize i = 0;  i < fileStatusArrayLen; i++) {
    jobject fileStatusObj = jniEnv->GetObjectArrayElement(fileStatusArray, i);

    if (fileStatusObj == nullptr) {
      return IOStatus::IOError("Exception when GetObjectArrayElement of FileStatus[], path: " + path);
    }

    jmethodID getPathMethodId = jniEnv->GetMethodID(
        cached_file_status_class_, "getPath", "()Lorg/apache/flink/core/fs/Path;");
    if (getPathMethodId == nullptr) {
      return IOStatus::IOError("Method getPathMethodId not found!");
    }

    jobject subPath = jniEnv->CallObjectMethod(fileStatusObj, getPathMethodId);
    if (subPath == nullptr) {
      return IOStatus::IOError("Exception when FileStatus#GetPath(), parent path: " + path);
    }

    jmethodID midToString = jniEnv->GetMethodID(cached_path_class_, "toString", "()Ljava/lang/String;");
    if (midToString == nullptr) {
      return IOStatus::IOError("Method Path#toString() not found!");
    }

    auto subPathStr = (jstring)jniEnv->CallObjectMethod(subPath, midToString);
    const char* str = jniEnv->GetStringUTFChars(subPathStr, nullptr);
    result->emplace_back(str);
    jniEnv->DeleteLocalRef(fileStatusObj);
    jniEnv->DeleteLocalRef(subPath);
    jniEnv->ReleaseStringUTFChars(subPathStr, str);
    jniEnv->DeleteLocalRef(subPathStr);
  }

  jniEnv->DeleteLocalRef(pathString);
  jniEnv->DeleteLocalRef(pathInstance);
  jniEnv->DeleteLocalRef(fileStatusArray);
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::DeleteDir(const std::string& name,
                                    const IOOptions& options,
                                    IODebugContext* dbg) {
  return Delete(name, options, dbg, true);
};

IOStatus FlinkFileSystem::DeleteFile(const std::string& fname,
                                        const IOOptions& options,
                                        IODebugContext* dbg) {
  return Delete(fname, options, dbg, false);
}

IOStatus FlinkFileSystem::Delete(
    const std::string& name, const IOOptions& options, IODebugContext* dbg, bool recursive) {
  if (FileExists(name, options, dbg).IsNotFound()){
    return IOStatus::PathNotFound("Could not find path: " + name);
  }
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jmethodID deleteMethodId = jniEnv->GetMethodID(
      file_system_class_, "delete", "(Lorg/apache/flink/core/fs/Path;Z)Z");
  if (deleteMethodId == nullptr) {
    return IOStatus::IOError("Method delete not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(name.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    return IOStatus::IOError("Could not create instance of class Path, path " + name);
  }

  jboolean created = jniEnv->CallBooleanMethod(
      file_system_instance_, deleteMethodId, pathInstance, recursive);
  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
  }

  jniEnv->DeleteLocalRef(pathString);
  jniEnv->DeleteLocalRef(pathInstance);
  return created ? IOStatus::OK() : IOStatus::IOError("Exception when Deleting, path: " + name);
}

IOStatus FlinkFileSystem::CreateDir(const std::string& name,
                                       const IOOptions& options,
                                       IODebugContext* dbg) {
  IOStatus s = FileExists(name, options, dbg);
  if (!s.ok()) {
    return CreateDirIfMissing(name, options, dbg);
  }
  return IOStatus::IOError("Dir " + name + " exists");
}

IOStatus FlinkFileSystem::CreateDirIfMissing(const std::string& name,
                                                const IOOptions& options,
                                                IODebugContext* dbg) {
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jmethodID mkdirMethodId = jniEnv->GetMethodID(
      file_system_class_, "mkdirs", "(Lorg/apache/flink/core/fs/Path;)Z");
  if (mkdirMethodId == nullptr) {
    return IOStatus::IOError("Method mkdirs not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(name.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    return IOStatus::IOError("Could not create instance of class Path, path: " + name);
  }

  jboolean created = jniEnv->CallBooleanMethod(
      file_system_instance_, mkdirMethodId, pathInstance);
  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
  }

  jniEnv->DeleteLocalRef(pathString);
  jniEnv->DeleteLocalRef(pathInstance);
  return created ? IOStatus::OK() : IOStatus::IOError("Exception when CreateDirIfMissing, path: " + name);
}

IOStatus FlinkFileSystem::GetFileSize(const std::string& fname,
                                         const IOOptions& options,
                                         uint64_t* size,
                                         IODebugContext* dbg) {
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jobject fileStatus;
  IOStatus status = GetFileStatus(fname, options, dbg, &fileStatus);
  if (!status.ok()) {
    return status;
  }

  jmethodID getLenMethodId = jniEnv->GetMethodID(cached_file_status_class_, "getLen", "()J");
  if (getLenMethodId == nullptr) {
    return IOStatus::IOError("Method getLenMethodId not found!");
  }
  jlong fileSize = jniEnv->CallLongMethod(fileStatus, getLenMethodId);

  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    return IOStatus::IOError("Exception when GetFileSize, path: " + fname);
  }

  jniEnv->DeleteLocalRef(fileStatus);
  *size = fileSize;
  return IOStatus::OK();
}

// 1. Must guarantee that FileStatus::getPath will not be used!
// 2. The life cycle of fileStatus is maintained by caller.
IOStatus FlinkFileSystem::GetFileStatus(
    const std::string& fname, const IOOptions& options, IODebugContext* dbg, jobject* fileStatus) {
  if (FileExists(fname, options, dbg).IsNotFound()){
    return IOStatus::PathNotFound("Could not find path: " + fname);
  }
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jmethodID getFileStatusMethodId = jniEnv->GetMethodID(
      file_system_class_, "getFileStatus", "(Lorg/apache/flink/core/fs/Path;)Lorg/apache/flink/core/fs/FileStatus;");
  if (getFileStatusMethodId == nullptr) {
    return IOStatus::IOError("Method getFileStatusMethodId not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(fname.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    return IOStatus::IOError("Could not create instance of class Path, path: " + fname);
  }

  *fileStatus = jniEnv->CallObjectMethod(
      file_system_instance_, getFileStatusMethodId, pathInstance);
  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    return IOStatus::IOError("Exception when GetFileStatus, path: " + fname);
  }

  jniEnv->DeleteLocalRef(pathString);
  jniEnv->DeleteLocalRef(pathInstance);
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::GetFileModificationTime(const std::string& fname,
                                                     const IOOptions& options,
                                                     uint64_t* time, 
                                                     IODebugContext* dbg) {
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jobject fileStatus;
  IOStatus status = GetFileStatus(fname, options, dbg, &fileStatus);
  if (!status.ok()) {
    return status;
  }

  jmethodID getModificationTimeMethodId = jniEnv->GetMethodID(cached_file_status_class_, "getModificationTime", "()J");
  if (getModificationTimeMethodId == nullptr) {
    return IOStatus::IOError("Method getModificationTimeMethodId not found!");
  }
  jlong fileModificationTime = jniEnv->CallLongMethod(fileStatus, getModificationTimeMethodId);

  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    return IOStatus::IOError("Exception when GetFileModificationTime, path: " + fname);
  }

  jniEnv->DeleteLocalRef(fileStatus);
  *time = fileModificationTime;
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::IsDirectory(const std::string& path,
                                      const IOOptions& options,
                                      bool* is_dir,
                                      IODebugContext* dbg) {

  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jobject fileStatus;
  IOStatus status = GetFileStatus(path, options, dbg, &fileStatus);
  if (!status.ok()) {
    return status;
  }

  jmethodID isDirMethodId = jniEnv->GetMethodID(cached_file_status_class_, "isDir", "()Z");
  if (isDirMethodId == nullptr) {
    return IOStatus::IOError("Method isDirMethodId not found!");
  }
  jboolean isDir = jniEnv->CallBooleanMethod(fileStatus, isDirMethodId);

  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    return IOStatus::IOError("Exception when IsDirectory, path: " + path);
  }

  jniEnv->DeleteLocalRef(fileStatus);
  *is_dir = isDir;
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::RenameFile(const std::string& src, const std::string& target,
                                        const IOOptions& options, IODebugContext* dbg) {
  if (FileExists(src, options, dbg).IsNotFound()){
    return IOStatus::PathNotFound("Could not find src path: " + src);
  }
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jmethodID renameMethodId = jniEnv->GetMethodID(
      file_system_class_, "rename", "(Lorg/apache/flink/core/fs/Path;Lorg/apache/flink/core/fs/Path;)Z");
  if (renameMethodId == nullptr) {
    return IOStatus::IOError("Method renameMethodId not found!");
  }

  jstring srcPathString = jniEnv->NewStringUTF(src.c_str());
  jobject srcPathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, srcPathString);
  if (srcPathInstance == nullptr) {
    return IOStatus::IOError("Could not create instance of class Path, path: " + src);
  }

  jstring targetPathString = jniEnv->NewStringUTF(target.c_str());
  jobject targetPathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, targetPathString);
  if (targetPathInstance == nullptr) {
    return IOStatus::IOError("Could not create instance of class Path, path: " + target);
  }

  jboolean renamed = jniEnv->CallBooleanMethod(
      file_system_instance_, renameMethodId, srcPathInstance, targetPathInstance);
  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    return IOStatus::IOError("Exception when RenameFile, src path: " + src + ", target path: " + target);
  }

  return renamed ? IOStatus::OK() :
                 IOStatus::IOError("Exception when RenameFile, src path: " + src + ", target path: " + target);
}

IOStatus FlinkFileSystem::LockFile(const std::string& /*fname*/,
                                      const IOOptions& /*options*/,
                                      FileLock** lock,
                                      IODebugContext* /*dbg*/) {
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

Status FlinkFileSystem::Create(const std::shared_ptr<FileSystem>& base, const std::string& uri, std::unique_ptr<FileSystem>* result) {
  auto * fileSystem = new FlinkFileSystem(base, uri);
  fileSystem->status();
  result->reset(fileSystem);
  return fileSystem->status();
}
}  // namespace ROCKSDB_NAMESPACE

