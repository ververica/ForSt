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

#include <fstream>
#include <iostream>

#define ASSERT_TRUE(expression)                                               \
  if (!(expression)) {                                                        \
    std::cerr << "Assertion failed: " << #expression << ", file " << __FILE__ \
              << ", line " << __LINE__ << "." << std::endl;                   \
    std::abort();                                                             \
  }

namespace ROCKSDB_NAMESPACE {

EnvFlinkTestSuites::EnvFlinkTestSuites(const std::string& basePath)
    : base_path_(basePath) {}

void EnvFlinkTestSuites::runAllTestSuites() {
  setUp();
  testFileExist();
}

void EnvFlinkTestSuites::setUp() {
  auto status = ROCKSDB_NAMESPACE::NewFlinkEnv(base_path_, &flink_env_);
  if (!status.ok()) {
    throw std::runtime_error("New FlinkEnv failed");
  }
}

void EnvFlinkTestSuites::testFileExist() {
  std::string fileName("test-file");
  Status result = flink_env_->FileExists(fileName);
  ASSERT_TRUE(result.IsNotFound());

  // Generate a file manually
  const std::string prefix = "file:";
  std::string writeFileName = base_path_ + fileName;
  if (writeFileName.compare(0, prefix.size(), prefix) == 0) {
    writeFileName = writeFileName.substr(prefix.size());
  }
  std::ofstream writeFile(writeFileName);
  writeFile << "testFileExist";
  writeFile.close();

  result = flink_env_->FileExists(fileName);
  ASSERT_TRUE(result.ok());
}
}  // namespace ROCKSDB_NAMESPACE