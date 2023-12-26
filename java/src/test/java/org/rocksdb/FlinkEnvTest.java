// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

public class FlinkEnvTest {

	@Test
	public void testLoadClass() {
		try (FlinkEnv flinkEnv = new FlinkEnv("flink://test-dir")) {
			flinkEnv.testLoadClass("java/lang/object");
			//It could also pass if dependency is in classpath
//			flinkEnv.testLoadClass("org/apache/flink/core/fs/local/LocalFileSystem");
		}
	}
}
