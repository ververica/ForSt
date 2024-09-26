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

package org.forstdb;

/**
 * Flink Env which proxy all filesystem access to Flink FileSystem.
 */
public class FlinkEnv extends Env {
  /**
   <p>Creates a new environment that is used for Flink environment.</p>
   *
   * <p>The caller must delete the result when it is
   * no longer needed.</p>
   *
   * @param basePath the base path string for the given Flink file system,
   * formatted as "{fs-schema-supported-by-flink}://xxx"
   */
  public FlinkEnv(final String basePath) {
    super(createFlinkEnv(basePath));
  }

  private static native long createFlinkEnv(final String basePath);

  @Override protected final native void disposeInternal(final long handle);
}