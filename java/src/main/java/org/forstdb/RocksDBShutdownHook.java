/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.forstdb;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A shutdown hook to join threads before JVM quit, to avoid deadlock
 * between VM threads and background threads.
 * See https://github.com/ververica/ForSt/pull/30
 */
public class RocksDBShutdownHook {
  private static final AtomicBoolean registered = new AtomicBoolean(false);

  public static boolean register() {
    if (registered.compareAndSet(false, true)) {
      return register(() -> {
        Env env = Env.getDefault();
        if (env != null) {
          env.setBackgroundThreads(0, Priority.LOW);
          env.setBackgroundThreads(0, Priority.HIGH);
        }
      }, "Joining background threads");
    }
    return false;
  }

  private static boolean register(AutoCloseable service, String serviceName) {
    final Thread shutdownHook = new Thread(() -> {
      try {
        service.close();
      } catch (Throwable t) {
        System.err.println("Error during shutdown of " + serviceName + " via JVM shutdown hook.");
        t.printStackTrace(System.err);
      }
    }, serviceName + " shutdown hook");

    try {
      // Add JVM shutdown hook to call shutdown of service
      Runtime.getRuntime().addShutdownHook(shutdownHook);
      return true;
    } catch (IllegalStateException e) {
      // JVM is already shutting down. no need to do our work
    } catch (Throwable t) {
      System.err.println(
          "Cannot register shutdown hook that cleanly terminates " + serviceName + ".");
      t.printStackTrace(System.err);
    }
    return false;
  }
}
