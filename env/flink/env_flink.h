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

#pragma once

#include "jni_helper.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// FlinkFileSystem extended from FileSystemWrapper which delegate necessary
// methods to Flink FileSystem based on JNI. For other methods, base FileSystem
// will proxy its methods.
class FlinkFileSystem : public FileSystemWrapper {
 public:
  // Create FlinkFileSystem with base_fs proxying all other methods and
  // base_path
  static Status Create(const std::shared_ptr<FileSystem>& /*base_fs*/,
                       const std::string& /*base_path*/,
                       std::unique_ptr<FileSystem>* /*fs*/,
                       jobject file_system_instance);

  // Define some names
  static const char* kClassName() { return "FlinkFileSystem"; }
  const char* Name() const override { return kClassName(); }
  static const char* kNickName() { return "flink"; }
  const char* NickName() const override { return kNickName(); }

  ~FlinkFileSystem() override;

  // Several methods current FileSystem must implement
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
  IOStatus FileExists(const std::string& /*file_name*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus GetChildren(const std::string& /*file_name*/,
                       const IOOptions& /*options*/,
                       std::vector<std::string>* /*result*/,
                       IODebugContext* /*dbg*/) override;
  IOStatus DeleteFile(const std::string& /*file_name*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override;
  IOStatus CreateDir(const std::string& /*name*/, const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override;
  IOStatus CreateDirIfMissing(const std::string& /*name*/,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override;
  IOStatus DeleteDir(const std::string& /*file_name*/,
                     const IOOptions& /*options*/,
                     IODebugContext* /*dbg*/) override;
  IOStatus GetFileSize(const std::string& /*file_name*/,
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

  IOStatus LinkFile(const std::string& /*src*/, const std::string& /*target*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override;

  IOStatus NewLogger(const std::string& fname, const IOOptions& io_opts,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override;

 private:
  const std::string base_path_;
  JavaClassCache* class_cache_;
  jobject file_system_instance_;

  explicit FlinkFileSystem(const std::shared_ptr<FileSystem>& base,
                           const std::string& fsname,
                           jobject file_system_instance);

  // Init FileSystem
  Status Init();

  IOStatus Delete(const std::string& /*file_name*/,
                  const IOOptions& /*options*/, IODebugContext* /*dbg*/,
                  bool /*recursive*/);
  IOStatus GetFileStatus(const std::string& /*file_name*/,
                         const IOOptions& /*options*/, IODebugContext* /*dbg*/,
                         jobject* /*fileStatus*/);
  std::string ConstructPath(const std::string& /*file_name*/);

  static std::string TrimTrailingSlash(const std::string& base_path) {
    if (!base_path.empty() && base_path.back() == '/') {
      return base_path.substr(0, base_path.size() - 1);
    } else {
      return base_path;
    }
  }
};

// Returns a `FlinkEnv` with base_path
Status NewFlinkEnv(const std::string& base_path, std::unique_ptr<Env>* env,
                   jobject file_system_instance);
// Returns a `FlinkFileSystem` with base_path
Status NewFlinkFileSystem(const std::string& base_path,
                          std::shared_ptr<FileSystem>* fs,
                          jobject file_system_instance);
}  // namespace ROCKSDB_NAMESPACE
