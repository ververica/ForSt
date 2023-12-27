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

// Finally, the FlinkFileSystem
FlinkFileSystem::FlinkFileSystem(const std::shared_ptr<FileSystem>& base, const std::string& fsname)
  : FileSystemWrapper(base), fsname_(fsname) {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
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

    file_system_instance_ = jniEnv->CallStaticObjectMethod(file_system_class_, methodId, uriInstance);
    if (file_system_instance_ == nullptr) {
      std::cerr << "Could not call static method get" << std::endl;
      return;
    }
    file_system_class_ = jniEnv->GetObjectClass(file_system_instance_);
    if (file_system_class_ == nullptr) {
      std::cerr << "Could not GetObjectClass of file_system_instance_" << std::endl;
      return;
    }

    jniEnv->DeleteLocalRef(uriStringArg);
    jniEnv->DeleteLocalRef(uriInstance);
    jniEnv->DeleteLocalRef(uriClass);
    jniEnv->DeleteLocalRef(abstract_file_system_class_);
}
  
FlinkFileSystem::~FlinkFileSystem() {
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv();
    if (file_system_instance_ != nullptr) {
      jniEnv->DeleteLocalRef(file_system_instance_);
    }
    if (file_system_class_ != nullptr) {
      jniEnv->DeleteLocalRef(file_system_class_);
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
  if (file_system_class_ == nullptr || file_system_instance_ == nullptr) {
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
    return IOStatus::IOError();
  }

  jclass pathClass = jniEnv->FindClass("org/apache/flink/core/fs/Path");
  if (pathClass == nullptr) {
    std::cerr << "Class " << "org/apache/flink/core/fs/Path not found!" << std::endl;
    return IOStatus::IOError();
  }

  jmethodID getMethodID = jniEnv->GetMethodID(pathClass, "<init>", "(Ljava/lang/String;)V");
  if (getMethodID == nullptr) {
    std::cerr << "No default constructor found for class Path" << std::endl;
    return IOStatus::IOError();
  }

  jstring pathString = jniEnv->NewStringUTF(fname.c_str());
  jobject pathInstance = jniEnv->NewObject(pathClass, getMethodID, pathString);
  if (pathInstance == nullptr) {
    std::cerr << "Could not create instance of class Path" << std::endl;
    return IOStatus::IOError();
  }

  jboolean exists = jniEnv->CallBooleanMethod(
      file_system_instance_, existsMethodId, pathInstance);
  if (jniEnv->ExceptionCheck()) {
    jniEnv->ExceptionDescribe();
    jniEnv->ExceptionClear();
  }

  jniEnv->DeleteLocalRef(pathClass);
  jniEnv->DeleteLocalRef(pathString);
  jniEnv->DeleteLocalRef(pathInstance);
  return exists == JNI_TRUE ? IOStatus::OK() : IOStatus::NotFound();
}

IOStatus FlinkFileSystem::GetChildren(const std::string& path,
                                         const IOOptions& options,
                                         std::vector<std::string>* result, 
                                        IODebugContext* dbg) {
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::DeleteFile(const std::string& fname,
                                        const IOOptions& options,
                                        IODebugContext* dbg) {
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::CreateDir(const std::string& name,
                                       const IOOptions& /*options*/,
                                       IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::CreateDirIfMissing(const std::string& name,
                                                const IOOptions& options,
                                                IODebugContext* dbg) {
  IOStatus s = FileExists(name, options, dbg);
  if (s.IsNotFound()) {
  //  Not atomic. state might change b/w hdfsExists and CreateDir.
    s = CreateDir(name, options, dbg);
  }
  return s;
}
  
IOStatus FlinkFileSystem::DeleteDir(const std::string& name,
                                       const IOOptions& options,
                                       IODebugContext* dbg) {
  return DeleteFile(name, options, dbg);
};

IOStatus FlinkFileSystem::GetFileSize(const std::string& fname,
                                         const IOOptions& /*options*/,
                                         uint64_t* size,
                                         IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::GetFileModificationTime(const std::string& fname,
                                                     const IOOptions& /*options*/,
                                                     uint64_t* time, 
                                                     IODebugContext* /*dbg*/) {
  return IOStatus::OK();

}

// The rename is not atomic. HDFS does not allow a renaming if the
// target already exists. So, we delete the target before attempting the
// rename.
IOStatus FlinkFileSystem::RenameFile(const std::string& src, const std::string& target,
                                        const IOOptions& options, IODebugContext* dbg) {
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::LockFile(const std::string& /*fname*/,
                                      const IOOptions& /*options*/,
                                      FileLock** lock,
                                      IODebugContext* /*dbg*/) {
  *lock = nullptr;
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::UnlockFile(FileLock* /*lock*/,
                                        const IOOptions& /*options*/,
                                        IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::NewLogger(const std::string& fname,
                                       const IOOptions& /*options*/,
                                       std::shared_ptr<Logger>* result,
                                       IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus FlinkFileSystem::IsDirectory(const std::string& path,
                                         const IOOptions& /*options*/, 
                                         bool* is_dir,
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

