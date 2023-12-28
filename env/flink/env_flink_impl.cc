//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "env_flink.h"

#include <rocksdb/env.h>

#include <stdio.h>
#include <time.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <sys/time.h>
#include <string>
#include "logging/logging.h"
#include "rocksdb/status.h"
#include "util/string_util.h"
#include "JvmUtils.h"

#define HDFS_EXISTS 0
#define HDFS_DOESNT_EXIST -1
#define HDFS_SUCCESS 0

//
// This file defines an HDFS environment for rocksdb. It uses the libhdfs
// api to access HDFS. All HDFS files created by one instance of rocksdb
// will reside on the same HDFS cluster.
//
namespace ROCKSDB_NAMESPACE {
namespace {

// assume that there is one global logger for now. It is not thread-safe,
// but need not be because the logger is initialized at db-open time.
static Logger* mylog = nullptr;
  
}  // namespace

// TODO: Release all jobjects when exception
// Finally, the FlinkFileSystem
FlinkFileSystem::FlinkFileSystem(const std::shared_ptr<FileSystem>& base, const std::string& fsname)
  : FileSystemWrapper(base), fsname_(fsname) {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();

    // Use Flink FileSystem.get(URI uri) to load real FileSystem (e.g. S3FileSystem/OSSFileSystem/...)
    jclass abstract_file_system_class_ = jniEnv->FindClass("org/apache/flink/core/fs/FileSystem");
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
    jclass fileSystemClass = jniEnv->GetObjectClass(file_system_instance_);
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

    cached_path_constructor_ = jniEnv->GetMethodID(pathClass, "<init>", "(Ljava/lang/String;)V");
    if (cached_path_constructor_ == nullptr) {
      std::cerr << "No default constructor found for class Path" << std::endl;
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
  return IOStatus::OK();
}

// open a file for random reading
IOStatus FlinkFileSystem::NewRandomAccessFile(const std::string& fname,
                                                 const FileOptions& options,
                                                 std::unique_ptr<FSRandomAccessFile>* result,
                                                 IODebugContext* dbg) {
  return IOStatus::OK();
}

// create a new file for writing
IOStatus FlinkFileSystem::NewWritableFile(const std::string& fname,
                                             const FileOptions& options,
                                             std::unique_ptr<FSWritableFile>* result,
                                             IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::NewDirectory(const std::string& name,
                                          const IOOptions& options,
                                          std::unique_ptr<FSDirectory>* result,
                                          IODebugContext* dbg) {
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::FileExists(const std::string& fname,
                                        const IOOptions& /*options*/,
                                        IODebugContext* /*dbg*/) {
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jmethodID existsMethodId = jniEnv->GetMethodID(
      file_system_class_, "exists", "(Lorg/apache/flink/core/fs/Path;)Z");
  if (existsMethodId == nullptr) {
    std::cerr << "Method exists not found!" << std::endl;
    return IOStatus::IOError("Method exists not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(fname.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    std::cerr << "Could not create instance of class Path" << std::endl;
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

// Not Efficient! Should cache some class and method or add more usable methods in FLink FileSystem
IOStatus FlinkFileSystem::GetChildren(const std::string& path,
                                         const IOOptions& options,
                                         std::vector<std::string>* result, 
                                        IODebugContext* dbg) {
  if (FileExists(path, options, dbg).IsNotFound()){
    return IOStatus::PathNotFound("Could not find path: " + path);
  }
  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jmethodID listStatusMethodId = jniEnv->GetMethodID(
      file_system_class_, "listStatus", "(LLorg/apache/flink/core/fs/Path;)[Lorg/apache/hadoop/fs/FileStatus;");
  if (listStatusMethodId == nullptr) {
    std::cerr << "Method listStatusMethodId not found!" << std::endl;
    return IOStatus::IOError("Method listStatusMethodId not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(path.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    std::cerr << "Could not create instance of class Path, path: " << path << std::endl;
    return IOStatus::IOError("Could not create instance of class Path, path: " + path);
  }

  auto fileStatusArray = (jobjectArray)jniEnv->CallObjectMethod(
      file_system_instance_, listStatusMethodId, pathInstance);
  if (fileStatusArray == nullptr) {
    std::cerr << "Exception when FileSystem#listStatus(Path f), path: " << path << std::endl;
    return IOStatus::IOError("Exception when FileSystem.listStatus(Path f), path: " + path);
  }

  jsize fileStatusArrayLen = jniEnv->GetArrayLength(fileStatusArray);
  for (jsize i = 0;  i < fileStatusArrayLen; i++) {
    jobject fileStatusObj = jniEnv->GetObjectArrayElement(fileStatusArray, i);

    if (fileStatusObj == nullptr) {
      std::cerr << "Exception when GetObjectArrayElement of FileStatus[], path: " << path << std::endl;
      return IOStatus::IOError("Exception when GetObjectArrayElement of FileStatus[], path: " + path);
    }

    jmethodID getPathMethodId = jniEnv->GetMethodID(
        cached_file_status_class_, "getPath", "()Lorg/apache/flink/core/fs/Path;");
    if (getPathMethodId == nullptr) {
      std::cerr << "Method getPathMethodId not found!" << std::endl;
      return IOStatus::IOError("Method getPathMethodId not found!");
    }

    jobject subPath = jniEnv->CallObjectMethod(fileStatusObj, getPathMethodId);
    if (subPath == nullptr) {
      std::cerr << "Exception when FileStatus#GetPath(), parent path: " << path << std::endl;
      return IOStatus::IOError("Exception when FileStatus#GetPath(), parent path: " + path);
    }

    jmethodID midToString = jniEnv->GetMethodID(cached_path_class_, "toString", "()Ljava/lang/String;");
    if (midToString == nullptr) {
      std::cerr << "Method Path#toString() not found!" << std::endl;
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
      file_system_class_, "delete", "(LLorg/apache/flink/core/fs/Path;Z)Z");
  if (deleteMethodId == nullptr) {
    std::cerr << "Method delete not found!" << std::endl;
    return IOStatus::IOError("Method delete not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(name.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    std::cerr << "Could not create instance of class Path, path: " << name << std::endl;
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
    std::cerr << "Method mkdirs not found!" << std::endl;
    return IOStatus::IOError("Method mkdirs not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(name.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    std::cerr << "Could not create instance of class Path, path: " << name << std::endl;
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
  jobject* fileStatus;
  IOStatus status = GetFileStatus(fname, options, dbg, fileStatus);
  if (!status.ok()) {
    return status;
  }

  jmethodID getLenMethodId = jniEnv->GetMethodID(cached_file_status_class_, "getLen", "()J");
  if (getLenMethodId == nullptr) {
    std::cerr << "Method getLenMethodId not found!" << std::endl;
    return IOStatus::IOError("Method getLenMethodId not found!");
  }
  jlong fileSize = jniEnv->CallLongMethod(*fileStatus, getLenMethodId);

  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    return IOStatus::IOError("Exception when GetFileSize, path: " + fname);
  }

  jniEnv->DeleteLocalRef(*fileStatus);
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
      file_system_class_, "getFileStatus", "(Lorg/apache/flink/core/fs/Path;)Lorg/apache/hadoop/fs/FileStatus;");
  if (getFileStatusMethodId == nullptr) {
    std::cerr << "Method getFileStatusMethodId not found!" << std::endl;
    return IOStatus::IOError("Method getFileStatusMethodId not found!");
  }

  jstring pathString = jniEnv->NewStringUTF(fname.c_str());
  jobject pathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, pathString);
  if (pathInstance == nullptr) {
    std::cerr << "Could not create instance of class Path, path: " << fname << std::endl;
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
  jobject* fileStatus;
  IOStatus status = GetFileStatus(fname, options, dbg, fileStatus);
  if (!status.ok()) {
    return status;
  }

  jmethodID getModificationTimeMethodId = jniEnv->GetMethodID(cached_file_status_class_, "getModificationTime", "()J");
  if (getModificationTimeMethodId == nullptr) {
    std::cerr << "Method getModificationTimeMethodId not found!" << std::endl;
    return IOStatus::IOError("Method getModificationTimeMethodId not found!");
  }
  jlong fileModificationTime = jniEnv->CallLongMethod(*fileStatus, getModificationTimeMethodId);

  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    return IOStatus::IOError("Exception when GetFileModificationTime, path: " + fname);
  }

  jniEnv->DeleteLocalRef(*fileStatus);
  *time = fileModificationTime;
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::IsDirectory(const std::string& path,
                                      const IOOptions& options,
                                      bool* is_dir,
                                      IODebugContext* dbg) {

  JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
  jobject* fileStatus;
  IOStatus status = GetFileStatus(path, options, dbg, fileStatus);
  if (!status.ok()) {
    return status;
  }

  jmethodID isDirMethodId = jniEnv->GetMethodID(cached_file_status_class_, "isDir", "()Z");
  if (isDirMethodId == nullptr) {
    std::cerr << "Method isDirMethodId not found!" << std::endl;
    return IOStatus::IOError("Method isDirMethodId not found!");
  }
  jboolean isDir = jniEnv->CallBooleanMethod(*fileStatus, isDirMethodId);

  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
    return IOStatus::IOError("Exception when IsDirectory, path: " + path);
  }

  jniEnv->DeleteLocalRef(*fileStatus);
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
    std::cerr << "Method renameMethodId not found!" << std::endl;
    return IOStatus::IOError("Method renameMethodId not found!");
  }

  jstring srcPathString = jniEnv->NewStringUTF(src.c_str());
  jobject srcPathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, srcPathString);
  if (srcPathInstance == nullptr) {
    std::cerr << "Could not create instance of class Path, path: " << src << std::endl;
    return IOStatus::IOError("Could not create instance of class Path, path: " + src);
  }

  jstring targetPathString = jniEnv->NewStringUTF(target.c_str());
  jobject targetPathInstance = jniEnv->NewObject(cached_path_class_, cached_path_constructor_, targetPathString);
  if (targetPathInstance == nullptr) {
    std::cerr << "Could not create instance of class Path, path: " << target << std::endl;
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

IOStatus FlinkFileSystem::NewLogger(const std::string& fname,
                                       const IOOptions& /*options*/,
                                       std::shared_ptr<Logger>* result,
                                       IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

Status FlinkFileSystem::Create(const std::shared_ptr<FileSystem>& base, const std::string& uri, std::unique_ptr<FileSystem>* result) {
  auto * fileSystem = new FlinkFileSystem(base, uri);
  fileSystem->status();
  result->reset(fileSystem);
  return fileSystem->status();
}
}  // namespace ROCKSDB_NAMESPACE

