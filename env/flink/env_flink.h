//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/status.h>
#include "jni.h"

namespace ROCKSDB_NAMESPACE {
class ObjectLibrary;
  
class FlinkFileSystem : public FileSystemWrapper {
 public:
  static const char* kClassName() { return "FlinkFileSystem"; }
  const char* Name() const override { return kClassName(); }
  static const char* kNickName() { return "flink"; }
  static constexpr const char* kProto = "flink://";
  
  const char* NickName() const override { return kNickName(); }
  static Status Create(const std::shared_ptr<FileSystem>& base, const std::string& fsname, std::unique_ptr<FileSystem>* fs);

  explicit FlinkFileSystem(const std::shared_ptr<FileSystem>& base, const std::string& fsname);
  ~FlinkFileSystem() override;

  std::string GetId() const override;
  
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;
  
  IOStatus NewSequentialFile(const std::string& /*fname*/,
                             const FileOptions& /*options*/,
                             std::unique_ptr<FSSequentialFile>* /*result*/,
                             IODebugContext* /*dbg*/) override;
  IOStatus NewRandomAccessFile(const std::string& /*fname*/,
                               const FileOptions& /*options*/,
                               std::unique_ptr<FSRandomAccessFile>* /*result*/,
                               IODebugContext* /*dbg*/) override;
  IOStatus NewWritableFile(const std::string& /*fname*/,
                           const FileOptions& /*options*/,
                           std::unique_ptr<FSWritableFile>* /*result*/,
                           IODebugContext* /*dbg*/) override;
  IOStatus NewDirectory(const std::string& /*name*/,
                        const IOOptions& /*options*/,
                        std::unique_ptr<FSDirectory>* /*result*/,
                        IODebugContext* /*dbg*/) override;
  IOStatus FileExists(const std::string& /*fname*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus GetChildren(const std::string& /*path*/,
                       const IOOptions& /*options*/,
                       std::vector<std::string>* /*result*/,
                       IODebugContext* /*dbg*/) override;
  IOStatus DeleteFile(const std::string& /*fname*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus CreateDir(const std::string& /*name*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override;
  IOStatus CreateDirIfMissing(const std::string& /*name*/,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override;
  IOStatus DeleteDir(const std::string& /*name*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override;
  IOStatus GetFileSize(const std::string& /*fname*/,
                       const IOOptions& /*options*/, uint64_t* /*size*/,
                       IODebugContext* /*dbg*/) override;
  IOStatus GetFileModificationTime(const std::string& /*fname*/,
                                   const IOOptions& /*options*/,
                                   uint64_t* /*time*/,
                                   IODebugContext* /*dbg*/) override;
  IOStatus RenameFile(const std::string& /*src*/, const std::string& /*target*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus LockFile(const std::string& /*fname*/, const IOOptions& /*options*/,
                    FileLock** /*lock*/, IODebugContext* /*dbg*/) override;
  IOStatus UnlockFile(FileLock* /*lock*/, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus IsDirectory(const std::string& /*path*/,
                       const IOOptions& /*options*/, bool* /*is_dir*/,
                       IODebugContext* /*dbg*/) override;
  Status status();

  static IOStatus throwJniException(const std::string& /*message*/);
 private:
  std::string fsname_;
  jclass file_system_class_;
  jobject file_system_instance_;

  // cache frequently-used class and method to reduce the cost of JNI
  jclass cached_path_class_;
  jmethodID cached_path_constructor_;
  jclass cached_file_status_class_;

  IOStatus Delete(const std::string& /*name*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/, bool /*recursive*/);
  IOStatus GetFileStatus(const std::string& /*fname*/,
                       const IOOptions& /*options*/,
                       IODebugContext* /*dbg*/, jobject* /*fileStatus*/);
  std::string ConstructPath(const std::string& /*fname*/);
};

// Returns a `FileSystem` that hashes file contents when naming files, thus
// deduping them. RocksDB however expects files to be identified based on a
// monotonically increasing counter, so a mapping of RocksDB's name to content
// hash is needed. This mapping is stored in a separate RocksDB instance.
Status NewFlinkEnv(const std::string& fsname, std::unique_ptr<Env>* env);
Status NewFlinkFileSystem(const std::string& fsname, std::shared_ptr<FileSystem>* fs);
extern "C" {
int register_FlinkObjects(ROCKSDB_NAMESPACE::ObjectLibrary& library, const std::string&);
void hdfs_reg();
} // extern "C"
}  // namespace ROCKSDB_NAMESPACE
