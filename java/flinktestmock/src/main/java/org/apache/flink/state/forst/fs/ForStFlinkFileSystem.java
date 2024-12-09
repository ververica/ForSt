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

package org.apache.flink.state.forst.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

/**
 * RemoteRocksdbFlinkFileSystem, used to expose flink fileSystem interface to frocksdb.
 */
public class ForStFlinkFileSystem extends FileSystem {
  private final FileSystem flinkFS;

  public ForStFlinkFileSystem(FileSystem flinkFS) {
    this.flinkFS = flinkFS;
  }

  public static FileSystem get(URI uri) throws IOException {
    return new ForStFlinkFileSystem(FileSystem.get(uri));
  }

  @Override
  public Path getWorkingDirectory() {
    return flinkFS.getWorkingDirectory();
  }

  @Override
  public Path getHomeDirectory() {
    return flinkFS.getHomeDirectory();
  }

  @Override
  public URI getUri() {
    return flinkFS.getUri();
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return flinkFS.getFileStatus(f);
  }

  @Override
  public ByteBufferReadableFSDataInputStream open(Path f, int bufferSize) throws IOException {
    InputStream original = flinkFS.open(f, bufferSize);
    long fileSize = flinkFS.getFileStatus(f).getLen();
    return new ByteBufferReadableFSDataInputStream(f, original, fileSize);
  }

  @Override
  public ByteBufferReadableFSDataInputStream open(Path f) throws IOException {
    InputStream original = flinkFS.open(f);
    long fileSize = flinkFS.getFileStatus(f).getLen();
    return new ByteBufferReadableFSDataInputStream(f, original, fileSize);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return flinkFS.listStatus(f);
  }

  @Override
  public boolean exists(final Path f) throws IOException {
    return flinkFS.exists(f);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return flinkFS.delete(f, recursive);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return flinkFS.mkdirs(f);
  }

  public ByteBufferWritableFSDataOutputStream create(Path f) throws IOException {
    return create(f, WriteMode.OVERWRITE);
  }

  @Override
  public ByteBufferWritableFSDataOutputStream create(Path f, WriteMode overwriteMode)
      throws IOException {
    OutputStream original = flinkFS.create(f, overwriteMode);
    return new ByteBufferWritableFSDataOutputStream(f, original);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    // The rename is not atomic for RocksDB. Some FileSystems e.g. HDFS, OSS does not allow a
    // renaming if the target already exists. So, we delete the target before attempting the
    // rename.
    if (flinkFS.exists(dst)) {
      boolean deleted = flinkFS.delete(dst, false);
      if (!deleted) {
        throw new IOException("Fail to delete dst path: " + dst);
      }
    }
    return flinkFS.rename(src, dst);
  }

  public int link(Path src, Path dst) throws IOException {
    // let forstdb copy the file
    return -1;
  }

  @Override
  public boolean isDistributedFS() {
    return flinkFS.isDistributedFS();
  }
}
