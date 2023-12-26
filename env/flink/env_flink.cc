//  Copyright (c) 2021-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "env_flink.h"

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/object_registry.h"


namespace ROCKSDB_NAMESPACE {

#ifndef ROCKSDB_LITE
int register_FlinkObjects(ObjectLibrary& library, const std::string&) {
  library.AddFactory<FileSystem>(ObjectLibrary::PatternEntry(FlinkFileSystem::kNickName(), false)
                                 .AddSeparator("://", false),
      [](const std::string& uri, std::unique_ptr<FileSystem>* guard,
         std::string* errmsg) {
        Status s = FlinkFileSystem::Create(FileSystem::Default(), uri, guard);
        if (!s.ok()) {
          *errmsg = "Failed to connect to default server";
        }
        return guard->get();
      });
  library.AddFactory<Env>(ObjectLibrary::PatternEntry(FlinkFileSystem::kNickName(), false)
                          .AddSeparator("://", false),
      [](const std::string& uri, std::unique_ptr<Env>* guard,
         std::string* errmsg) {
        Status s = NewFlinkEnv(uri, guard);
        if (!s.ok()) {
          *errmsg = "Failed to connect to default server";
        }
        return guard->get();
      });
  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
  
}
#endif // ROCKSDB_LITE
void hdfs_reg() {
#ifndef ROCKSDB_LITE
  auto lib = ObjectRegistry::Default()->AddLibrary("flink");
  register_FlinkObjects(*lib, "flink");
#endif // ROCKSDB_LITE
}

// The factory method for creating an Flink Env
Status NewFlinkFileSystem(const std::string& uri, std::shared_ptr<FileSystem>* fs) {
  std::unique_ptr<FileSystem> flinkFileSystem;
  Status s = FlinkFileSystem::Create(FileSystem::Default(), uri, &flinkFileSystem);
  if (s.ok()) {
    fs->reset(flinkFileSystem.release());
  }
  return s;
}

Status NewFlinkEnv(const std::string& uri, std::unique_ptr<Env>* flinkFileSystem) {
  std::shared_ptr<FileSystem> fs;
  Status s = NewFlinkFileSystem(uri, &fs);
  if (s.ok()) {
    *flinkFileSystem =  NewCompositeEnv(fs);
  }
  return s;
}


}  // namespace ROCKSDB_NAMESPACE
