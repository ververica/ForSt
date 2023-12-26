/**
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

#include <jni.h>

/**
 * Encapsulates logic to cache jclass objects so they can re-used across
 * calls to FindClass. Creating jclass objects every time libhdfs has to
 * invoke a method can hurt performance. By cacheing jclass objects we avoid
 * this overhead.
 *
 * We use the term "cached" here loosely; jclasses are not truly cached,
 * instead they are created once during JVM load and are kept alive until the
 * process shutdowns. There is no eviction of jclass objects.
 *
 * @see https://www.ibm.com/developerworks/library/j-jni/index.html#notc
 */

/**
 * Each enum value represents one jclass that is cached. Enum values should
 * be passed to getJclass or getName to get the jclass object or class name
 * represented by the enum value.
 */
typedef enum {
    JC_URI,
    JC_BYTE_BUFFER,
    JC_ENUM_SET,
    JC_EXCEPTION_UTILS,
    JC_CFUTURE,
    // A special marker enum that counts the number of cached jclasses
    NUM_CACHED_CLASSES
} CachedJavaClass;

/**
 * Return the jclass object represented by the given CachedJavaClass
 */
jclass getJclass(CachedJavaClass cachedJavaClass);

/**
 * Return the class name represented by the given CachedJavaClass
 */
const char *getClassName(CachedJavaClass cachedJavaClass);

/* Some frequently used Java class names */
#define JAVA_NET_ISA    "java/net/InetSocketAddress"
#define JAVA_NET_URI    "java/net/URI"
#define JAVA_BYTEBUFFER "java/nio/ByteBuffer"
#define JAVA_STRING     "java/lang/String"
#define JAVA_ENUMSET    "java/util/EnumSet"
#define JAVA_CFUTURE    "java/util/concurrent/CompletableFuture"
#define JAVA_TIMEUNIT   "java/util/concurrent/TimeUnit"
#define JAVA_OBJECT     "java/lang/Object"

/* Some frequently used third-party class names */

#define EXCEPTION_UTILS "org/apache/commons/lang3/exception/ExceptionUtils"

