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

package org.rocksdb;

public class EnvFlinkTestSuite implements AutoCloseable {
  private final String basePath;

  private final long nativeObjectHandle;

  public EnvFlinkTestSuite(String basePath) {
    this.basePath = basePath;
    this.nativeObjectHandle = buildNativeObject(basePath);
  }

  private native long buildNativeObject(String basePath);

  private native void runAllTestSuites(long nativeObjectHandle);

  private native void disposeInternal(long nativeObjectHandle);

  public void runAllTestSuites() {
    runAllTestSuites(nativeObjectHandle);
  }

  @Override
  public void close() throws Exception {
    disposeInternal(nativeObjectHandle);
  }
}