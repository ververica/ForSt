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
  : FileSystemWrapper(base), fsname_(fsname){
}
  
FlinkFileSystem::~FlinkFileSystem() {

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
  return IOStatus::OK();
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
  result->reset(new FlinkFileSystem(base, uri));
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE

