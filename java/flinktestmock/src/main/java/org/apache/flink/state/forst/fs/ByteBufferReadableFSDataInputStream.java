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
import java.nio.ByteBuffer;
import org.apache.flink.core.fs.LocalDataInputStream;
import org.apache.flink.core.fs.Path;

/**
 * ByteBufferReadableFSDataInputStream.
 */
public class ByteBufferReadableFSDataInputStream extends InputStream {
  private final LocalDataInputStream localDataInputStream;
  private final Path path;
  private final long totalFileSize;

  public ByteBufferReadableFSDataInputStream(
      Path path, InputStream inputStream, long totalFileSize) {
    if (!(inputStream instanceof LocalDataInputStream)) {
      throw new UnsupportedOperationException("Unsupported input stream type");
    }
    this.localDataInputStream = (LocalDataInputStream) inputStream;
    this.path = path;
    this.totalFileSize = totalFileSize;
  }

  public void seek(long desired) throws IOException {
    localDataInputStream.seek(desired);
  }

  public long getPos() throws IOException {
    return localDataInputStream.getPos();
  }

  @Override
  public int read() throws IOException {
    return localDataInputStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return localDataInputStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return localDataInputStream.read(b, off, len);
  }

  /**
   * Return the total number of bytes read into the buffer.
   * REQUIRES: External synchronization
   */
  public int readFully(ByteBuffer bb) throws IOException {
    return readFullyFromFSDataInputStream(localDataInputStream, bb);
  }

  private int readFullyFromFSDataInputStream(LocalDataInputStream fsdis, ByteBuffer bb)
      throws IOException {
    byte[] tmp = new byte[bb.remaining()];
    int n = 0;
    long pos = fsdis.getPos();
    while (n < tmp.length) {
      int read = fsdis.read(tmp, n, tmp.length - n);
      if (read == -1) {
        break;
      }
      n += read;
    }
    if (n > 0) {
      bb.put(tmp, 0, n);
    }
    return n;
  }

  /**
   * Return the total number of bytes read into the buffer.
   * Safe for concurrent use by multiple threads.
   */
  public int readFully(long position, ByteBuffer bb) throws IOException {
    localDataInputStream.seek(position);
    return readFullyFromFSDataInputStream(localDataInputStream, bb);
  }

  @Override
  public long skip(long n) throws IOException {
    seek(getPos() + n);
    return getPos();
  }

  @Override
  public int available() throws IOException {
    return localDataInputStream.available();
  }

  @Override
  public void close() throws IOException {
    localDataInputStream.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    localDataInputStream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    localDataInputStream.reset();
  }

  @Override
  public boolean markSupported() {
    return localDataInputStream.markSupported();
  }
}
