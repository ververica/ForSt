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

/*
 * This file is based on source code from the Hadoop Project (http://hadoop.apache.org/), licensed
 * by the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.
 */

package org.apache.flink.core.fs;

import static org.apache.flink.core.fs.LocalFileSystem.LOCAL_URI;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;

/**
 * Abstract base class of all file systems used by Flink. This class may be extended to implement
 * distributed file systems, or local file systems. The abstraction by this file system is very
 * simple, and the set of available operations quite limited, to support the common denominator of a
 * wide range of file systems. For example, appending to or mutating existing files is not
 * supported.
 */
public abstract class FileSystem {
  /**
   * The possible write modes. The write mode decides what happens if a file should be created,
   * but already exists.
   */
  public enum WriteMode {

    /**
     * Creates the target file only if no file exists at that path already. Does not overwrite
     * existing files and directories.
     */
    NO_OVERWRITE,

    /**
     * Creates a new target file regardless of any existing files or directories. Existing files
     * and directories will be deleted (recursively) automatically before creating the new file.
     */
    OVERWRITE
  }

  /**
   * Returns a reference to the {@link FileSystem} instance for accessing the local file system.
   *
   * @return a reference to the {@link FileSystem} instance for accessing the local file system.
   */
  public static FileSystem getLocalFileSystem() {
    return LocalFileSystem.getSharedInstance();
  }

  /**
   * Returns a reference to the {@link FileSystem} instance for accessing the file system
   * identified by the given {@link URI}.
   *
   * @param uri the {@link URI} identifying the file system
   * @return a reference to the {@link FileSystem} instance for accessing the file system
   *     identified by the given {@link URI}.
   * @throws IOException thrown if a reference to the file system instance could not be obtained
   */
  public static FileSystem get(URI uri) throws IOException {
    if (Objects.equals(LOCAL_URI.getScheme(), uri.getScheme())
        && Objects.equals(LOCAL_URI.getAuthority(), LOCAL_URI.getAuthority())) {
      return getLocalFileSystem();
    }
    throw new UnsupportedOperationException("Unsupported URI pattern:" + uri);
  }

  // ------------------------------------------------------------------------
  //  File System Methods
  // ------------------------------------------------------------------------

  /**
   * Returns the path of the file system's current working directory.
   *
   * @return the path of the file system's current working directory
   */
  public abstract Path getWorkingDirectory();

  /**
   * Returns the path of the user's home directory in this file system.
   *
   * @return the path of the user's home directory in this file system.
   */
  public abstract Path getHomeDirectory();

  /**
   * Returns a URI whose scheme and authority identify this file system.
   *
   * @return a URI whose scheme and authority identify this file system
   */
  public abstract URI getUri();

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist; IOException see specific
   *     implementation
   */
  public abstract FileStatus getFileStatus(Path f) throws IOException;

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public abstract InputStream open(Path f, int bufferSize) throws IOException;

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param f the file to open
   */
  public abstract InputStream open(Path f) throws IOException;

  /**
   * List the statuses of the files/directories in the given path if the path is a directory.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given path
   * @throws IOException
   */
  public abstract FileStatus[] listStatus(Path f) throws IOException;

  /**
   * Check if exists.
   *
   * @param f source file
   */
  public boolean exists(final Path f) throws IOException {
    try {
      return (getFileStatus(f) != null);
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Delete a file.
   *
   * @param f the path to delete
   * @param recursive if path is a directory and set to <code>true</code>, the directory is
   *     deleted else throws an exception. In case of a file the recursive can be set to either
   *     <code>true</code> or <code>false</code>
   * @return <code>true</code> if delete is successful, <code>false</code> otherwise
   * @throws IOException
   */
  public abstract boolean delete(Path f, boolean recursive) throws IOException;

  /**
   * Make the given file and all non-existent parents into directories. Has the semantics of Unix
   * 'mkdir -p'. Existence of the directory hierarchy is not an error.
   *
   * @param f the directory/directories to be created
   * @return <code>true</code> if at least one new directory has been created, <code>false</code>
   *     otherwise
   * @throws IOException thrown if an I/O error occurs while creating the directory
   */
  public abstract boolean mkdirs(Path f) throws IOException;

  /**
   * Opens an FSDataOutputStream at the indicated Path.
   *
   * <p>This method is deprecated, because most of its parameters are ignored by most file
   * systems. To control for example the replication factor and block size in the Hadoop
   * Distributed File system, make sure that the respective Hadoop configuration file is either
   * linked from the Flink configuration, or in the classpath of either Flink or the user code.
   *
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true, the file will be
   *     overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize the size of the file blocks
   * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
   *     file already exists at that path and the write mode indicates to not overwrite the file.
   * @deprecated Deprecated because not well supported across types of file systems. Control the
   *     behavior of specific file systems via configurations instead.
   */
  @Deprecated
  public OutputStream create(Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize) throws IOException {
    return create(f, overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE);
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path.
   *
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true, the file will be
   *     overwritten, and if false an error will be thrown.
   * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
   *     file already exists at that path and the write mode indicates to not overwrite the file.
   * @deprecated Use {@link #create(Path, WriteMode)} instead.
   */
  @Deprecated
  public OutputStream create(Path f, boolean overwrite) throws IOException {
    return create(f, overwrite ? WriteMode.OVERWRITE : WriteMode.NO_OVERWRITE);
  }

  /**
   * Opens an FSDataOutputStream to a new file at the given path.
   *
   * <p>If the file already exists, the behavior depends on the given {@code WriteMode}. If the
   * mode is set to {@link WriteMode#NO_OVERWRITE}, then this method fails with an exception.
   *
   * @param f The file path to write to
   * @param overwriteMode The action to take if a file or directory already exists at the given
   *     path.
   * @return The stream to the new file at the target path.
   * @throws IOException Thrown, if the stream could not be opened because of an I/O, or because a
   *     file already exists at that path and the write mode indicates to not overwrite the file.
   */
  public abstract OutputStream create(Path f, WriteMode overwriteMode) throws IOException;

  /**
   * Renames the file/directory src to dst.
   *
   * @param src the file/directory to rename
   * @param dst the new name of the file/directory
   * @return <code>true</code> if the renaming was successful, <code>false</code> otherwise
   * @throws IOException
   */
  public abstract boolean rename(Path src, Path dst) throws IOException;

  /**
   * Returns true if this is a distributed file system. A distributed file system here means that
   * the file system is shared among all Flink processes that participate in a cluster or job and
   * that all these processes can see the same files.
   *
   * @return True, if this is a distributed file system, false otherwise.
   */
  public abstract boolean isDistributedFS();
}
