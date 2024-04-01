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

#include "env/flink/env_flink_test_suite.h"

#include <algorithm>
#include <fstream>
#include <iostream>

#define ASSERT_TRUE(expression)                                               \
  if (!(expression)) {                                                        \
    std::cerr << "Assertion failed: " << #expression << ", file " << __FILE__ \
              << ", line " << __LINE__ << "." << std::endl;                   \
    std::abort();                                                             \
  }

#define ASSERT_FALSE(condition) ASSERT_TRUE(!(condition))

#define LOG(message) (std::cout << (message) << std::endl)

namespace ROCKSDB_NAMESPACE {

EnvFlinkTestSuites::EnvFlinkTestSuites(const std::string& basePath)
    : base_path_(basePath) {}

void EnvFlinkTestSuites::runAllTestSuites() {
  setUp();
  LOG("Stage 1: setUp OK");
  testDirOperation();
  LOG("Stage 2: testDirOperation OK");
  testFileOperation();
  LOG("Stage 3: testFileOperation OK");
  testGetChildren();
  LOG("Stage 4: testGetChildren OK");
  testFileReadAndWrite();
  LOG("Stage 5: testFileReadAndWrite OK");
}

void EnvFlinkTestSuites::setUp() {
  auto status = ROCKSDB_NAMESPACE::NewFlinkEnv(base_path_, &flink_env_);
  if (!status.ok()) {
    throw std::runtime_error("New FlinkEnv failed");
  }
}

void EnvFlinkTestSuites::testDirOperation() {
  const std::string dir_name = "test-dir";
  ASSERT_TRUE(flink_env_->FileExists(dir_name).IsNotFound());
  ASSERT_TRUE(flink_env_->CreateDir(dir_name).ok());
  ASSERT_TRUE(flink_env_->CreateDirIfMissing(dir_name).ok());
  ASSERT_FALSE(flink_env_->CreateDir(dir_name).ok());

  bool is_dir;
  ASSERT_TRUE(flink_env_->IsDirectory(dir_name, &is_dir).ok() && is_dir);
  ASSERT_TRUE(flink_env_->FileExists(dir_name).ok());
  ASSERT_TRUE(flink_env_->DeleteDir(dir_name).ok());
  ASSERT_TRUE(flink_env_->FileExists(dir_name).IsNotFound());
}

void EnvFlinkTestSuites::testFileOperation() {
  const std::string file_name = "test-file";
  const std::string not_exist_file_name = "not-exist-file";

  // test file exists
  ASSERT_TRUE(flink_env_->FileExists(file_name).IsNotFound());
  generateFile(file_name);
  ASSERT_TRUE(flink_env_->FileExists(file_name).ok());

  // test file status
  uint64_t file_size, file_mtime;
  ASSERT_TRUE(flink_env_->GetFileSize(file_name, &file_size).ok());
  ASSERT_FALSE(flink_env_->GetFileSize(not_exist_file_name, &file_size).ok());
  ASSERT_TRUE(file_size > 0);
  ASSERT_TRUE(flink_env_->GetFileModificationTime(file_name, &file_mtime).ok());
  ASSERT_FALSE(
      flink_env_->GetFileModificationTime(not_exist_file_name, &file_mtime)
          .ok());
  ASSERT_TRUE(file_mtime > 0);

  // test renaming file
  const std::string file_name_2 = "test-file-2";
  flink_env_->RenameFile(file_name, file_name_2);
  ASSERT_TRUE(flink_env_->FileExists(file_name).IsNotFound());
  ASSERT_TRUE(flink_env_->FileExists(file_name_2).ok());
  ASSERT_TRUE(flink_env_->DeleteFile(file_name_2).ok());
  ASSERT_TRUE(flink_env_->FileExists(file_name_2).IsNotFound());
}

void EnvFlinkTestSuites::testGetChildren() {
  const std::string dir_name = "test-dir";
  const std::string sub_dir_name = dir_name + "/test-sub-dir";
  const std::string file_name_1 = dir_name + "/test-file-1";
  const std::string file_name_2 = dir_name + "/test-file-2";
  ASSERT_TRUE(flink_env_->CreateDirIfMissing(dir_name).ok());
  ASSERT_TRUE(flink_env_->CreateDirIfMissing(sub_dir_name).ok());
  generateFile(file_name_1);
  generateFile(file_name_2);

  std::vector<std::string> result,
      expected{base_path_ + sub_dir_name, base_path_ + file_name_1,
               base_path_ + file_name_2};
  ASSERT_TRUE(flink_env_->GetChildren(dir_name, &result).ok());
  ASSERT_TRUE(result.size() == 3);
  std::sort(result.begin(), result.end());
  std::sort(expected.begin(), expected.end());
  ASSERT_TRUE(expected == result);
}

void EnvFlinkTestSuites::testFileReadAndWrite() {
  const std::string file_name = "test-file";
  const std::string content1 = "Hello World", content2 = ", Hello ForSt",
                    content = content1 + content2;

  std::unique_ptr<WritableFile> write_result;
  ASSERT_TRUE(
      flink_env_->NewWritableFile(file_name, &write_result, EnvOptions()).ok());
  write_result->Append(content1);
  write_result->Append(content2);
  write_result->Sync();
  write_result->Flush();
  write_result->Close();

  std::unique_ptr<SequentialFile> sequential_result;
  ASSERT_TRUE(
      flink_env_->NewSequentialFile(file_name, &sequential_result, EnvOptions())
          .ok());

  Slice sequential_data;
  char* sequential_scratch = new char[content2.size()];
  sequential_result->Skip(content1.size());
  sequential_result->Read(content2.size(), &sequential_data,
                          sequential_scratch);
  ASSERT_TRUE(sequential_data.data() == content2);
  delete[] sequential_scratch;

  std::unique_ptr<RandomAccessFile> random_access_result;
  ASSERT_TRUE(
      flink_env_
          ->NewRandomAccessFile(file_name, &random_access_result, EnvOptions())
          .ok());
  Slice random_access_data;
  char* random_access_scratch = new char[content2.size()];
  random_access_result->Read(content1.size(), content.size() - content1.size(),
                             &random_access_data, (char*)random_access_scratch);
  ASSERT_TRUE(random_access_data.data() == content2);
  delete[] random_access_scratch;
}

void EnvFlinkTestSuites::generateFile(const std::string& fileName) {
  // Generate a file manually
  const std::string prefix = "file:";
  std::string writeFileName = base_path_ + fileName;
  if (writeFileName.compare(0, prefix.size(), prefix) == 0) {
    writeFileName = writeFileName.substr(prefix.size());
  }
  std::ofstream writeFile(writeFileName);
  writeFile << "Hello World";
  writeFile.close();
}
}  // namespace ROCKSDB_NAMESPACE