// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Flink env which proxy all filesystem access to Flink FileSystem.
 */
public class FlinkEnv extends Env {

  /**
   <p>Creates a new environment that is used for Flink environment.</p>
   *
   * <p>The caller must delete the result when it is
   * no longer needed.</p>
   *
   * @param fsName the HDFS as a string in the form "flink://hostname:port/"
   */
  public FlinkEnv(final String fsName) {
    super(createFlinkEnv(fsName));
  }

  public void testLoadClass(final String className) {
    testLoadClass(nativeHandle_, className);
  }

  public boolean testFileExits(final String path) {
    return testFileExits(nativeHandle_, path);
  }

  private static native boolean testFileExits(final long handle, final String path);

  private static native void testLoadClass(final long handle, final String className);

  private static native long createFlinkEnv(final String fsName);
  @Override protected final native void disposeInternal(final long handle);
}
