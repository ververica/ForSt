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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.flink.core.fs.LocalDataOutputStream;
import org.apache.flink.core.fs.Path;

/**
 * ByteBufferWritableFSDataOutputStream.
 */
public class ByteBufferWritableFSDataOutputStream extends OutputStream {
  private final Path path;
  private final LocalDataOutputStream localDataOutputStream;

  public ByteBufferWritableFSDataOutputStream(Path path, OutputStream fsdos) {
    if (!(fsdos instanceof LocalDataOutputStream)) {
      throw new UnsupportedOperationException("Unsupported output stream type");
    }
    this.path = path;
    this.localDataOutputStream = (LocalDataOutputStream) fsdos;
  }

  public long getPos() throws IOException {
    return localDataOutputStream.getPos();
  }

  @Override
  public void write(int b) throws IOException {
    localDataOutputStream.write(b);
  }

  public void write(byte[] b) throws IOException {
    localDataOutputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    localDataOutputStream.write(b, off, len);
  }

  public void write(ByteBuffer bb) throws IOException {
    if (bb.hasArray()) {
      write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
    } else {
      byte[] tmp = new byte[bb.remaining()];
      bb.get(tmp);
      write(tmp, 0, tmp.length);
    }
  }

  @Override
  public void flush() throws IOException {
    localDataOutputStream.flush();
  }

  public void sync() throws IOException {
    localDataOutputStream.sync();
  }

  @Override
  public void close() throws IOException {
    localDataOutputStream.close();
  }
}
