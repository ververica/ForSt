NATIVE_JAVA_CLASSES = \
	org.forstdb.AbstractCompactionFilter\
	org.forstdb.AbstractCompactionFilterFactory\
	org.forstdb.AbstractComparator\
	org.forstdb.AbstractEventListener\
	org.forstdb.AbstractSlice\
	org.forstdb.AbstractTableFilter\
	org.forstdb.AbstractTraceWriter\
	org.forstdb.AbstractTransactionNotifier\
	org.forstdb.AbstractWalFilter\
	org.forstdb.BackupEngine\
	org.forstdb.BackupEngineOptions\
	org.forstdb.BlockBasedTableConfig\
	org.forstdb.BloomFilter\
	org.forstdb.Checkpoint\
	org.forstdb.ClockCache\
	org.forstdb.Cache\
	org.forstdb.CassandraCompactionFilter\
	org.forstdb.CassandraValueMergeOperator\
	org.forstdb.ColumnFamilyHandle\
	org.forstdb.ColumnFamilyOptions\
	org.forstdb.CompactionJobInfo\
	org.forstdb.CompactionJobStats\
	org.forstdb.CompactionOptions\
	org.forstdb.CompactionOptionsFIFO\
	org.forstdb.CompactionOptionsUniversal\
	org.forstdb.CompactRangeOptions\
	org.forstdb.ComparatorOptions\
	org.forstdb.CompressionOptions\
	org.forstdb.ConfigOptions\
	org.forstdb.DBOptions\
	org.forstdb.DirectSlice\
	org.forstdb.Env\
	org.forstdb.EnvOptions\
	org.forstdb.FlinkCompactionFilter\
	org.forstdb.FlushOptions\
	org.forstdb.Filter\
	org.forstdb.IngestExternalFileOptions\
	org.forstdb.HashLinkedListMemTableConfig\
	org.forstdb.HashSkipListMemTableConfig\
	org.forstdb.ConcurrentTaskLimiter\
	org.forstdb.ConcurrentTaskLimiterImpl\
	org.forstdb.KeyMayExist\
	org.forstdb.Logger\
	org.forstdb.LRUCache\
	org.forstdb.MemoryUsageType\
	org.forstdb.MemoryUtil\
	org.forstdb.MergeOperator\
	org.forstdb.NativeComparatorWrapper\
	org.forstdb.OptimisticTransactionDB\
	org.forstdb.OptimisticTransactionOptions\
	org.forstdb.Options\
	org.forstdb.OptionsUtil\
	org.forstdb.PersistentCache\
	org.forstdb.PerfContext\
	org.forstdb.PerfLevel\
	org.forstdb.PlainTableConfig\
	org.forstdb.RateLimiter\
	org.forstdb.ReadOptions\
	org.forstdb.RemoveEmptyValueCompactionFilter\
	org.forstdb.RestoreOptions\
	org.forstdb.RocksCallbackObject\
	org.forstdb.RocksDB\
	org.forstdb.RocksEnv\
	org.forstdb.RocksIterator\
	org.forstdb.RocksMemEnv\
	org.forstdb.SkipListMemTableConfig\
	org.forstdb.Slice\
	org.forstdb.SstFileManager\
	org.forstdb.SstFileWriter\
	org.forstdb.SstFileReader\
	org.forstdb.SstFileReaderIterator\
	org.forstdb.SstPartitionerFactory\
	org.forstdb.SstPartitionerFixedPrefixFactory\
	org.forstdb.Statistics\
	org.forstdb.ThreadStatus\
	org.forstdb.TimedEnv\
	org.forstdb.Transaction\
	org.forstdb.TransactionDB\
	org.forstdb.TransactionDBOptions\
	org.forstdb.TransactionOptions\
	org.forstdb.TransactionLogIterator\
	org.forstdb.TtlDB\
	org.forstdb.VectorMemTableConfig\
	org.forstdb.Snapshot\
	org.forstdb.StringAppendOperator\
	org.forstdb.UInt64AddOperator\
	org.forstdb.WriteBatch\
	org.forstdb.WriteBatch.Handler\
	org.forstdb.WriteOptions\
	org.forstdb.WriteBatchWithIndex\
	org.forstdb.WriteBufferManager\
	org.forstdb.WBWIRocksIterator

NATIVE_JAVA_TEST_CLASSES = \
    org.forstdb.RocksDBExceptionTest\
    org.forstdb.test.TestableEventListener\
    org.forstdb.NativeComparatorWrapperTest.NativeStringComparatorWrapper\
    org.forstdb.WriteBatchTest\
    org.forstdb.WriteBatchTestInternalHelper

ROCKSDB_MAJOR = $(shell grep -E "ROCKSDB_MAJOR.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_MINOR = $(shell grep -E "ROCKSDB_MINOR.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)
ROCKSDB_PATCH = $(shell grep -E "ROCKSDB_PATCH.[0-9]" ../include/rocksdb/version.h | cut -d ' ' -f 3)

NATIVE_INCLUDE = ./include
ARCH := $(shell getconf LONG_BIT)
SHA256_CMD ?= sha256sum

JAVA_TESTS = \
	org.forstdb.BackupEngineOptionsTest\
	org.forstdb.BackupEngineTest\
	org.forstdb.BlobOptionsTest\
	org.forstdb.BlockBasedTableConfigTest\
	org.forstdb.BuiltinComparatorTest\
	org.forstdb.ByteBufferUnsupportedOperationTest\
	org.forstdb.BytewiseComparatorRegressionTest\
	org.forstdb.util.BytewiseComparatorTest\
	org.forstdb.util.BytewiseComparatorIntTest\
	org.forstdb.CheckPointTest\
	org.forstdb.ClockCacheTest\
	org.forstdb.ColumnFamilyOptionsTest\
	org.forstdb.ColumnFamilyTest\
	org.forstdb.CompactionFilterFactoryTest\
	org.forstdb.CompactionJobInfoTest\
	org.forstdb.CompactionJobStatsTest\
	org.forstdb.CompactionOptionsTest\
	org.forstdb.CompactionOptionsFIFOTest\
	org.forstdb.CompactionOptionsUniversalTest\
	org.forstdb.CompactionPriorityTest\
	org.forstdb.CompactionStopStyleTest\
	org.forstdb.ComparatorOptionsTest\
	org.forstdb.CompressionOptionsTest\
	org.forstdb.CompressionTypesTest\
	org.forstdb.DBOptionsTest\
	org.forstdb.DirectSliceTest\
	org.forstdb.util.EnvironmentTest\
	org.forstdb.EnvOptionsTest\
	org.forstdb.EventListenerTest\
	org.forstdb.IngestExternalFileOptionsTest\
	org.forstdb.util.IntComparatorTest\
	org.forstdb.util.JNIComparatorTest\
	org.forstdb.FilterTest\
	org.forstdb.FlushTest\
	org.forstdb.ImportColumnFamilyTest\
	org.forstdb.InfoLogLevelTest\
	org.forstdb.KeyExistsTest \
	org.forstdb.KeyMayExistTest\
	org.forstdb.ConcurrentTaskLimiterTest\
	org.forstdb.LoggerTest\
	org.forstdb.LRUCacheTest\
	org.forstdb.MemoryUtilTest\
	org.forstdb.MemTableTest\
	org.forstdb.MergeCFVariantsTest\
	org.forstdb.MergeTest\
	org.forstdb.MergeVariantsTest\
	org.forstdb.MultiColumnRegressionTest \
	org.forstdb.MultiGetManyKeysTest\
	org.forstdb.MultiGetTest\
	org.forstdb.MixedOptionsTest\
	org.forstdb.MutableColumnFamilyOptionsTest\
	org.forstdb.MutableDBOptionsTest\
	org.forstdb.MutableOptionsGetSetTest \
	org.forstdb.NativeComparatorWrapperTest\
	org.forstdb.NativeLibraryLoaderTest\
	org.forstdb.OptimisticTransactionTest\
	org.forstdb.OptimisticTransactionDBTest\
	org.forstdb.OptimisticTransactionOptionsTest\
	org.forstdb.OptionsUtilTest\
	org.forstdb.OptionsTest\
	org.forstdb.PerfLevelTest \
	org.forstdb.PerfContextTest \
	org.forstdb.PutCFVariantsTest\
	org.forstdb.PutVariantsTest\
	org.forstdb.PlainTableConfigTest\
	org.forstdb.RateLimiterTest\
	org.forstdb.ReadOnlyTest\
	org.forstdb.ReadOptionsTest\
	org.forstdb.util.ReverseBytewiseComparatorIntTest\
	org.forstdb.RocksDBTest\
	org.forstdb.RocksDBExceptionTest\
	org.forstdb.DefaultEnvTest\
	org.forstdb.RocksIteratorTest\
	org.forstdb.RocksMemEnvTest\
	org.forstdb.util.SizeUnitTest\
	org.forstdb.SecondaryDBTest\
	org.forstdb.SliceTest\
	org.forstdb.SnapshotTest\
	org.forstdb.SstFileManagerTest\
	org.forstdb.SstFileWriterTest\
	org.forstdb.SstFileReaderTest\
	org.forstdb.SstPartitionerTest\
	org.forstdb.TableFilterTest\
	org.forstdb.TimedEnvTest\
	org.forstdb.TransactionTest\
	org.forstdb.TransactionDBTest\
	org.forstdb.TransactionOptionsTest\
	org.forstdb.TransactionDBOptionsTest\
	org.forstdb.TransactionLogIteratorTest\
	org.forstdb.TtlDBTest\
	org.forstdb.StatisticsTest\
	org.forstdb.StatisticsCollectorTest\
	org.forstdb.VerifyChecksumsTest\
	org.forstdb.WalFilterTest\
	org.forstdb.WALRecoveryModeTest\
	org.forstdb.WriteBatchHandlerTest\
	org.forstdb.WriteBatchTest\
	org.forstdb.WriteBatchThreadedTest\
	org.forstdb.WriteOptionsTest\
	org.forstdb.WriteBatchWithIndexTest

FLINK_TESTS =  \
    org.forstdb.flink.FlinkEnvTest

MAIN_SRC = src/main/java
TEST_SRC = src/test/java
OUTPUT = target
MAIN_CLASSES = $(OUTPUT)/classes
TEST_CLASSES = $(OUTPUT)/test-classes
JAVADOC = $(OUTPUT)/apidocs

BENCHMARK_MAIN_SRC = benchmark/src/main/java
BENCHMARK_OUTPUT = benchmark/target
BENCHMARK_MAIN_CLASSES = $(BENCHMARK_OUTPUT)/classes

SAMPLES_MAIN_SRC = samples/src/main/java
SAMPLES_OUTPUT = samples/target
SAMPLES_MAIN_CLASSES = $(SAMPLES_OUTPUT)/classes

JAVA_TEST_LIBDIR = test-libs
JAVA_JUNIT_VER = 4.13.1
JAVA_JUNIT_SHA256 = c30719db974d6452793fe191b3638a5777005485bae145924044530ffa5f6122
JAVA_JUNIT_JAR = junit-$(JAVA_JUNIT_VER).jar
JAVA_JUNIT_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_JUNIT_JAR)
JAVA_HAMCREST_VER = 2.2
JAVA_HAMCREST_SHA256 = 5e62846a89f05cd78cd9c1a553f340d002458380c320455dd1f8fc5497a8a1c1
JAVA_HAMCREST_JAR = hamcrest-$(JAVA_HAMCREST_VER).jar
JAVA_HAMCREST_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_HAMCREST_JAR)
JAVA_MOCKITO_VER = 1.10.19
JAVA_MOCKITO_SHA256 = d1a7a7ef14b3db5c0fc3e0a63a81b374b510afe85add9f7984b97911f4c70605
JAVA_MOCKITO_JAR = mockito-all-$(JAVA_MOCKITO_VER).jar
JAVA_MOCKITO_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_MOCKITO_JAR)
JAVA_CGLIB_VER = 3.3.0
JAVA_CGLIB_SHA256 = 9fe0c26d7464140ccdfe019ac687be1fb906122b508ab54beb810db0f09a9212
JAVA_CGLIB_JAR = cglib-$(JAVA_CGLIB_VER).jar
JAVA_CGLIB_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_CGLIB_JAR)
JAVA_ASSERTJ_VER = 2.9.0
JAVA_ASSERTJ_SHA256 = 5e88ea3ecbe3c48aa1346fec76c84979fa9c8d22499f11479011691230e8babf
JAVA_ASSERTJ_JAR = assertj-core-$(JAVA_ASSERTJ_VER).jar
JAVA_ASSERTJ_JAR_PATH = $(JAVA_TEST_LIBDIR)/$(JAVA_ASSERTJ_JAR)
JAVA_TESTCLASSPATH = $(JAVA_JUNIT_JAR_PATH):$(JAVA_HAMCREST_JAR_PATH):$(JAVA_MOCKITO_JAR_PATH):$(JAVA_CGLIB_JAR_PATH):$(JAVA_ASSERTJ_JAR_PATH)

MVN_LOCAL = ~/.m2/repository

# Set the path of the java commands
ifeq ($(JAVA_CMD),)
ifneq ($(JAVA_HOME),)
JAVA_CMD := $(JAVA_HOME)/bin/java
else
JAVA_CMD := java
endif
endif

ifeq ($(JAVAC_CMD),)
ifneq ($(JAVA_HOME),)
JAVAC_CMD := $(JAVA_HOME)/bin/javac
else
JAVAC_CMD := javac
endif
endif

ifeq ($(JAVADOC_CMD),)
ifneq ($(JAVA_HOME),)
JAVADOC_CMD := $(JAVA_HOME)/bin/javadoc
else
JAVADOC_CMD := javadoc
endif
endif

MAVEN_CMD := mvn

# Look for the Java version (1.6->6, 1.7->7, 1.8->8, 11.0->11, 13.0->13, 15.0->15 etc..)
JAVAC_VERSION := $(shell $(JAVAC_CMD) -version 2>&1)
JAVAC_MAJOR_VERSION := $(word 2,$(subst ., ,$(JAVAC_VERSION)))
ifeq ($(JAVAC_MAJOR_VERSION),1)
JAVAC_MAJOR_VERSION := $(word 3,$(subst ., ,$(JAVAC_VERSION)))
endif

# Test whether the version we see meets our minimum
MIN_JAVAC_MAJOR_VERSION := 8
JAVAC_VERSION_GE_MIN := $(shell [ $(JAVAC_MAJOR_VERSION) -ge $(MIN_JAVAC_MAJOR_VERSION) ] > /dev/null 2>&1 && echo true)

# Set the default JAVA_ARGS to "" for DEBUG_LEVEL=0
JAVA_ARGS ?=

JAVAC_ARGS ?=

# Read plugin configuration
PLUGIN_PATH = ../plugin
ROCKSDB_PLUGIN_MKS = $(foreach plugin, $(ROCKSDB_PLUGINS), $(PLUGIN_PATH)/$(plugin)/*.mk)
include $(ROCKSDB_PLUGIN_MKS)

# Add paths to Java sources in plugins
ROCKSDB_PLUGIN_JAVA_ROOTS = $(foreach plugin, $(ROCKSDB_PLUGINS), $(PLUGIN_PATH)/$(plugin)/java)
PLUGIN_SOURCES = $(foreach root, $(ROCKSDB_PLUGIN_JAVA_ROOTS), $(foreach pkg, org/forstdb/util org/forstdb, $(root)/$(MAIN_SRC)/$(pkg)/*.java))
CORE_SOURCES = $(foreach pkg, org/forstdb/util org/forstdb, $(MAIN_SRC)/$(pkg)/*.java)
SOURCES = $(wildcard $(CORE_SOURCES) $(PLUGIN_SOURCES))
PLUGIN_TEST_SOURCES = $(foreach root, $(ROCKSDB_PLUGIN_JAVA_ROOTS), $(foreach pkg, org/forstdb/test org/forstdb/util org/forstdb, $(root)/$(TEST_SRC)/$(pkg)/*.java))
CORE_TEST_SOURCES = $(foreach pkg, org/forstdb/test org/forstdb/util org/forstdb/flink org/forstdb, $(TEST_SRC)/$(pkg)/*.java)
TEST_SOURCES = $(wildcard $(CORE_TEST_SOURCES) $(PLUGIN_TEST_SOURCES))
MOCK_FLINK_TEST_SOURCES = $(foreach pkg, org/apache/flink/core/fs  org/apache/flink/state/forst/fs, flinktestmock/src/main/java/$(pkg)/*.java)

# Configure the plugin tests and java classes
ROCKSDB_PLUGIN_NATIVE_JAVA_CLASSES = $(foreach plugin, $(ROCKSDB_PLUGINS), $(foreach class, $($(plugin)_NATIVE_JAVA_CLASSES), $(class)))
NATIVE_JAVA_CLASSES = $(NATIVE_JAVA_CLASSES) $(ROCKSDB_PLUGIN_NATIVE_JAVA_CLASSES)
ROCKSDB_PLUGIN_JAVA_TESTS = $(foreach plugin, $(ROCKSDB_PLUGINS), $(foreach testclass, $($(plugin)_JAVA_TESTS), $(testclass)))
ALL_JAVA_TESTS = $(FLINK_TESTS) $(JAVA_TESTS) $(ROCKSDB_PLUGIN_JAVA_TESTS)

# When debugging add -Xcheck:jni to the java args
ifneq ($(DEBUG_LEVEL),0)
	JAVA_ARGS += -ea -Xcheck:jni
	JAVAC_ARGS += -Xlint:deprecation -Xlint:unchecked
endif

# Using a Facebook AWS account for S3 storage. (maven.org has a history
# of failing in Travis builds.)
DEPS_URL?=https://rocksdb-deps.s3-us-west-2.amazonaws.com/jars

java-version:
ifneq ($(JAVAC_VERSION_GE_MIN),true)
	echo 'Java version is $(JAVAC_VERSION), minimum required version is $(MIN_JAVAC_MAJOR_VERSION)'
	exit 1
endif

clean: clean-not-downloaded clean-downloaded

clean-not-downloaded:
	$(AM_V_at)rm -rf $(NATIVE_INCLUDE)
	$(AM_V_at)rm -rf $(OUTPUT)
	$(AM_V_at)rm -rf $(BENCHMARK_OUTPUT)
	$(AM_V_at)rm -rf $(SAMPLES_OUTPUT)

clean-downloaded:
	$(AM_V_at)rm -rf $(JAVA_TEST_LIBDIR)


javadocs: java
	$(AM_V_GEN)mkdir -p $(JAVADOC)
	$(AM_V_at)$(JAVADOC_CMD) -d $(JAVADOC) -sourcepath $(MAIN_SRC) -subpackages org

javalib: java java_test javadocs

java: java-version
	$(AM_V_GEN)mkdir -p $(MAIN_CLASSES)
	$(AM_V_at) $(JAVAC_CMD) $(JAVAC_ARGS) -h $(NATIVE_INCLUDE) -d $(MAIN_CLASSES) $(SOURCES)

sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)$(JAVAC_CMD) $(JAVAC_ARGS) -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/RocksDBSample.java
	$(AM_V_at)@rm -rf /tmp/forstjni
	$(AM_V_at)@rm -rf /tmp/forstjni_not_found
	$(JAVA_CMD) $(JAVA_ARGS) -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) RocksDBSample /tmp/forstjni
	$(AM_V_at)@rm -rf /tmp/forstjni
	$(AM_V_at)@rm -rf /tmp/forstjni_not_found

column_family_sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)$(JAVAC_CMD) $(JAVAC_ARGS) -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/RocksDBColumnFamilySample.java
	$(AM_V_at)@rm -rf /tmp/forstjni
	$(JAVA_CMD) $(JAVA_ARGS) -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) RocksDBColumnFamilySample /tmp/forstjni
	$(AM_V_at)@rm -rf /tmp/forstjni

transaction_sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)$(JAVAC_CMD) -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/TransactionSample.java
	$(AM_V_at)@rm -rf /tmp/forstjni
	$(JAVA_CMD) -ea -Xcheck:jni -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) TransactionSample /tmp/forstjni
	$(AM_V_at)@rm -rf /tmp/forstjni

optimistic_transaction_sample: java
	$(AM_V_GEN)mkdir -p $(SAMPLES_MAIN_CLASSES)
	$(AM_V_at)$(JAVAC_CMD) -cp $(MAIN_CLASSES) -d $(SAMPLES_MAIN_CLASSES) $(SAMPLES_MAIN_SRC)/OptimisticTransactionSample.java
	$(AM_V_at)@rm -rf /tmp/forstjni
	$(JAVA_CMD) -ea -Xcheck:jni -Djava.library.path=target -cp $(MAIN_CLASSES):$(SAMPLES_MAIN_CLASSES) OptimisticTransactionSample /tmp/forstjni
	$(AM_V_at)@rm -rf /tmp/forstjni

$(JAVA_TEST_LIBDIR):
	mkdir -p "$(JAVA_TEST_LIBDIR)"

$(JAVA_JUNIT_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/junit/junit/$(JAVA_JUNIT_VER)/$(JAVA_JUNIT_JAR)))
	cp -v $(MVN_LOCAL)/junit/junit/$(JAVA_JUNIT_VER)/$(JAVA_JUNIT_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output $(JAVA_JUNIT_JAR_PATH) --location $(DEPS_URL)/$(JAVA_JUNIT_JAR)
	JAVA_JUNIT_SHA256_ACTUAL=`$(SHA256_CMD) $(JAVA_JUNIT_JAR_PATH) | cut -d ' ' -f 1`; \
	if [ "$(JAVA_JUNIT_SHA256)" != "$$JAVA_JUNIT_SHA256_ACTUAL" ]; then \
		echo $(JAVA_JUNIT_JAR_PATH) checksum mismatch, expected=\"$(JAVA_JUNIT_SHA256)\" actual=\"$$JAVA_JUNIT_SHA256_ACTUAL\"; \
		exit 1; \
	fi
endif

$(JAVA_HAMCREST_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/org/hamcrest/hamcrest/$(JAVA_HAMCREST_VER)/$(JAVA_HAMCREST_JAR)))
	cp -v $(MVN_LOCAL)/org/hamcrest/hamcrest/$(JAVA_HAMCREST_VER)/$(JAVA_HAMCREST_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output $(JAVA_HAMCREST_JAR_PATH) --location $(DEPS_URL)/$(JAVA_HAMCREST_JAR)
	JAVA_HAMCREST_SHA256_ACTUAL=`$(SHA256_CMD) $(JAVA_HAMCREST_JAR_PATH) | cut -d ' ' -f 1`; \
	if [ "$(JAVA_HAMCREST_SHA256)" != "$$JAVA_HAMCREST_SHA256_ACTUAL" ]; then \
		echo $(JAVA_HAMCREST_JAR_PATH) checksum mismatch, expected=\"$(JAVA_HAMCREST_SHA256)\" actual=\"$$JAVA_HAMCREST_SHA256_ACTUAL\"; \
		exit 1; \
	fi
endif

$(JAVA_MOCKITO_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/org/mockito/mockito-all/$(JAVA_MOCKITO_VER)/$(JAVA_MOCKITO_JAR)))
	cp -v $(MVN_LOCAL)/org/mockito/mockito-all/$(JAVA_MOCKITO_VER)/$(JAVA_MOCKITO_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output "$(JAVA_MOCKITO_JAR_PATH)" --location $(DEPS_URL)/$(JAVA_MOCKITO_JAR)
	JAVA_MOCKITO_SHA256_ACTUAL=`$(SHA256_CMD) $(JAVA_MOCKITO_JAR_PATH) | cut -d ' ' -f 1`; \
	if [ "$(JAVA_MOCKITO_SHA256)" != "$$JAVA_MOCKITO_SHA256_ACTUAL" ]; then \
		echo $(JAVA_MOCKITO_JAR_PATH) checksum mismatch, expected=\"$(JAVA_MOCKITO_SHA256)\" actual=\"$$JAVA_MOCKITO_SHA256_ACTUAL\"; \
		exit 1; \
	fi
endif

$(JAVA_CGLIB_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/cglib/cglib/$(JAVA_CGLIB_VER)/$(JAVA_CGLIB_JAR)))
	cp -v $(MVN_LOCAL)/cglib/cglib/$(JAVA_CGLIB_VER)/$(JAVA_CGLIB_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output "$(JAVA_CGLIB_JAR_PATH)" --location $(DEPS_URL)/$(JAVA_CGLIB_JAR)
	JAVA_CGLIB_SHA256_ACTUAL=`$(SHA256_CMD) $(JAVA_CGLIB_JAR_PATH) | cut -d ' ' -f 1`; \
	if [ "$(JAVA_CGLIB_SHA256)" != "$$JAVA_CGLIB_SHA256_ACTUAL" ]; then \
		echo $(JAVA_CGLIB_JAR_PATH) checksum mismatch, expected=\"$(JAVA_CGLIB_SHA256)\" actual=\"$$JAVA_CGLIB_SHA256_ACTUAL\"; \
		exit 1; \
	fi
endif

$(JAVA_ASSERTJ_JAR_PATH): $(JAVA_TEST_LIBDIR)
ifneq (,$(wildcard $(MVN_LOCAL)/org/assertj/assertj-core/$(JAVA_ASSERTJ_VER)/$(JAVA_ASSERTJ_JAR)))
	cp -v $(MVN_LOCAL)/org/assertj/assertj-core/$(JAVA_ASSERTJ_VER)/$(JAVA_ASSERTJ_JAR) $(JAVA_TEST_LIBDIR)
else
	curl --fail --insecure --output "$(JAVA_ASSERTJ_JAR_PATH)" --location $(DEPS_URL)/$(JAVA_ASSERTJ_JAR)
	JAVA_ASSERTJ_SHA256_ACTUAL=`$(SHA256_CMD) $(JAVA_ASSERTJ_JAR_PATH) | cut -d ' ' -f 1`; \
	if [ "$(JAVA_ASSERTJ_SHA256)" != "$$JAVA_ASSERTJ_SHA256_ACTUAL" ]; then \
		echo $(JAVA_ASSERTJ_JAR_PATH) checksum mismatch, expected=\"$(JAVA_ASSERTJ_SHA256)\" actual=\"$$JAVA_ASSERTJ_SHA256_ACTUAL\"; \
		exit 1; \
	fi
endif

resolve_test_deps: $(JAVA_JUNIT_JAR_PATH) $(JAVA_HAMCREST_JAR_PATH) $(JAVA_MOCKITO_JAR_PATH) $(JAVA_CGLIB_JAR_PATH) $(JAVA_ASSERTJ_JAR_PATH)

java_test: java resolve_test_deps
	$(AM_V_GEN)mkdir -p $(TEST_CLASSES)
	$(AM_V_at) $(JAVAC_CMD) $(JAVAC_ARGS) -cp $(MAIN_CLASSES):$(JAVA_TESTCLASSPATH) -h $(NATIVE_INCLUDE) -d $(TEST_CLASSES)\
		$(TEST_SOURCES)

test: java mock_flink_fs java_test
	$(MAKE) run_test

run_test:
	$(JAVA_CMD) $(JAVA_ARGS) -Djava.library.path=target -cp "$(MAIN_CLASSES):$(TEST_CLASSES):$(JAVA_TESTCLASSPATH):target/*" org.forstdb.test.RocksJunitRunner $(ALL_JAVA_TESTS)
	$(JAVA_CMD) $(JAVA_ARGS) -Djava.library.path=target -cp "$(MAIN_CLASSES):$(TEST_CLASSES):$(JAVA_TESTCLASSPATH):target/*" org.forstdb.test.RocksJunitRunner org.forstdb.StatisticsTest

run_plugin_test:
	$(JAVA_CMD) $(JAVA_ARGS) -Djava.library.path=target -cp "$(MAIN_CLASSES):$(TEST_CLASSES):$(JAVA_TESTCLASSPATH):target/*" org.forstdb.test.RocksJunitRunner $(ROCKSDB_PLUGIN_JAVA_TESTS)

db_bench: java
	$(AM_V_GEN)mkdir -p $(BENCHMARK_MAIN_CLASSES)
	$(AM_V_at)$(JAVAC_CMD) $(JAVAC_ARGS) -cp $(MAIN_CLASSES) -d $(BENCHMARK_MAIN_CLASSES) $(BENCHMARK_MAIN_SRC)/org/forstdb/benchmark/*.java

pmd:
	$(MAVEN_CMD) pmd:pmd pmd:cpd pmd:check

mock_flink_fs:
	$(AM_V_at) $(JAVAC_CMD) $(JAVAC_ARGS) -cp $(MAIN_CLASSES):$(JAVA_TESTCLASSPATH) -h $(NATIVE_INCLUDE) -d $(TEST_CLASSES) \
		$(MOCK_FLINK_TEST_SOURCES)

flink_test: java java_test mock_flink_fs
	$(MAKE) run_flink_test

run_flink_test:
	$(JAVA_CMD) $(JAVA_ARGS) -Djava.library.path=target -cp "$(MAIN_CLASSES):$(TEST_CLASSES):$(JAVA_TESTCLASSPATH):target/*" org.forstdb.test.RocksJunitRunner $(FLINK_TESTS)
