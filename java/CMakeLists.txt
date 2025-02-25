cmake_minimum_required(VERSION 3.4)

set(JAVA_JUNIT_VERSION "4.13.1")
set(JAVA_HAMCR_VERSION "2.2")
set(JAVA_MOCKITO_VERSION "1.10.19")
set(JAVA_CGLIB_VERSION "3.3.0")
set(JAVA_ASSERTJ_VERSION "2.9.0")


if(${CMAKE_VERSION} VERSION_LESS "3.11.4")
    message("Please consider switching to CMake 3.11.4 or newer")
endif()

set(CMAKE_JAVA_COMPILE_FLAGS -source 8)

set(JNI_NATIVE_SOURCES
        forstjni/backup_engine_options.cc
        forstjni/backupenginejni.cc
        forstjni/cassandra_compactionfilterjni.cc
        forstjni/cassandra_value_operator.cc
        forstjni/checkpoint.cc
        forstjni/clock_cache.cc
        forstjni/cache.cc
        forstjni/columnfamilyhandle.cc
        forstjni/compaction_filter.cc
        forstjni/compaction_filter_factory.cc
        forstjni/compaction_filter_factory_jnicallback.cc
        forstjni/compaction_job_info.cc
        forstjni/compaction_job_stats.cc
        forstjni/compaction_options.cc
        forstjni/compaction_options_fifo.cc
        forstjni/compaction_options_universal.cc
        forstjni/compact_range_options.cc
        forstjni/comparator.cc
        forstjni/comparatorjnicallback.cc
        forstjni/compression_options.cc
        forstjni/concurrent_task_limiter.cc
        forstjni/config_options.cc
        forstjni/env.cc
        forstjni/env_flink.cc
        forstjni/env_flink_test_suite.cc
        forstjni/env_options.cc
        forstjni/event_listener.cc
        forstjni/event_listener_jnicallback.cc
        forstjni/export_import_files_metadatajni.cc
        forstjni/flink_compactionfilterjni.cc
        forstjni/filter.cc
        forstjni/import_column_family_options.cc
        forstjni/hyper_clock_cache.cc
        forstjni/ingest_external_file_options.cc
        forstjni/iterator.cc
        forstjni/jnicallback.cc
        forstjni/loggerjnicallback.cc
        forstjni/lru_cache.cc
        forstjni/memory_util.cc
        forstjni/memtablejni.cc
        forstjni/merge_operator.cc
        forstjni/native_comparator_wrapper_test.cc
        forstjni/optimistic_transaction_db.cc
        forstjni/optimistic_transaction_options.cc
        forstjni/options.cc
        forstjni/options_util.cc
        forstjni/persistent_cache.cc
        forstjni/jni_perf_context.cc
        forstjni/ratelimiterjni.cc
        forstjni/remove_emptyvalue_compactionfilterjni.cc
        forstjni/restorejni.cc
        forstjni/rocks_callback_object.cc
        forstjni/rocksdb_exception_test.cc
        forstjni/rocksjni.cc
        forstjni/slice.cc
        forstjni/snapshot.cc
        forstjni/sst_file_manager.cc
        forstjni/sst_file_writerjni.cc
        forstjni/sst_file_readerjni.cc
        forstjni/sst_file_reader_iterator.cc
        forstjni/sst_partitioner.cc
        forstjni/statistics.cc
        forstjni/statisticsjni.cc
        forstjni/table.cc
        forstjni/table_filter.cc
        forstjni/table_filter_jnicallback.cc
        forstjni/testable_event_listener.cc
        forstjni/thread_status.cc
        forstjni/trace_writer.cc
        forstjni/trace_writer_jnicallback.cc
        forstjni/transaction.cc
        forstjni/transaction_db.cc
        forstjni/transaction_db_options.cc
        forstjni/transaction_log.cc
        forstjni/transaction_notifier.cc
        forstjni/transaction_notifier_jnicallback.cc
        forstjni/transaction_options.cc
        forstjni/ttl.cc
        forstjni/wal_filter.cc
        forstjni/wal_filter_jnicallback.cc
        forstjni/write_batch.cc
        forstjni/writebatchhandlerjnicallback.cc
        forstjni/write_batch_test.cc
        forstjni/write_batch_with_index.cc
        forstjni/write_buffer_manager.cc
)

set(JAVA_MAIN_CLASSES
        src/main/java/org/forstdb/AbstractCompactionFilter.java
        src/main/java/org/forstdb/AbstractCompactionFilterFactory.java
        src/main/java/org/forstdb/AbstractComparator.java
        src/main/java/org/forstdb/AbstractEventListener.java
        src/main/java/org/forstdb/AbstractImmutableNativeReference.java
        src/main/java/org/forstdb/AbstractMutableOptions.java
        src/main/java/org/forstdb/AbstractNativeReference.java
        src/main/java/org/forstdb/AbstractRocksIterator.java
        src/main/java/org/forstdb/AbstractSlice.java
        src/main/java/org/forstdb/AbstractTableFilter.java
        src/main/java/org/forstdb/AbstractTraceWriter.java
        src/main/java/org/forstdb/AbstractTransactionNotifier.java
        src/main/java/org/forstdb/AbstractWalFilter.java
        src/main/java/org/forstdb/AbstractWriteBatch.java
        src/main/java/org/forstdb/AccessHint.java
        src/main/java/org/forstdb/AdvancedColumnFamilyOptionsInterface.java
        src/main/java/org/forstdb/AdvancedMutableColumnFamilyOptionsInterface.java
        src/main/java/org/forstdb/BackgroundErrorReason.java
        src/main/java/org/forstdb/BackupEngineOptions.java
        src/main/java/org/forstdb/BackupEngine.java
        src/main/java/org/forstdb/BackupInfo.java
        src/main/java/org/forstdb/BlockBasedTableConfig.java
        src/main/java/org/forstdb/BloomFilter.java
        src/main/java/org/forstdb/BuiltinComparator.java
        src/main/java/org/forstdb/ByteBufferGetStatus.java
        src/main/java/org/forstdb/Cache.java
        src/main/java/org/forstdb/CassandraCompactionFilter.java
        src/main/java/org/forstdb/CassandraValueMergeOperator.java
        src/main/java/org/forstdb/Checkpoint.java
        src/main/java/org/forstdb/ChecksumType.java
        src/main/java/org/forstdb/ClockCache.java
        src/main/java/org/forstdb/ColumnFamilyDescriptor.java
        src/main/java/org/forstdb/ColumnFamilyHandle.java
        src/main/java/org/forstdb/ColumnFamilyMetaData.java
        src/main/java/org/forstdb/ColumnFamilyOptionsInterface.java
        src/main/java/org/forstdb/ColumnFamilyOptions.java
        src/main/java/org/forstdb/CompactionJobInfo.java
        src/main/java/org/forstdb/CompactionJobStats.java
        src/main/java/org/forstdb/CompactionOptions.java
        src/main/java/org/forstdb/CompactionOptionsFIFO.java
        src/main/java/org/forstdb/CompactionOptionsUniversal.java
        src/main/java/org/forstdb/CompactionPriority.java
        src/main/java/org/forstdb/CompactionReason.java
        src/main/java/org/forstdb/CompactRangeOptions.java
        src/main/java/org/forstdb/CompactionStopStyle.java
        src/main/java/org/forstdb/CompactionStyle.java
        src/main/java/org/forstdb/ComparatorOptions.java
        src/main/java/org/forstdb/ComparatorType.java
        src/main/java/org/forstdb/CompressionOptions.java
        src/main/java/org/forstdb/CompressionType.java
        src/main/java/org/forstdb/ConfigOptions.java
        src/main/java/org/forstdb/DataBlockIndexType.java
        src/main/java/org/forstdb/DBOptionsInterface.java
        src/main/java/org/forstdb/DBOptions.java
        src/main/java/org/forstdb/DbPath.java
        src/main/java/org/forstdb/DirectSlice.java
        src/main/java/org/forstdb/EncodingType.java
        src/main/java/org/forstdb/Env.java
        src/main/java/org/forstdb/EnvFlinkTestSuite.java
        src/main/java/org/forstdb/EnvOptions.java
        src/main/java/org/forstdb/EventListener.java
        src/main/java/org/forstdb/Experimental.java
        src/main/java/org/forstdb/ExportImportFilesMetaData.java
        src/main/java/org/forstdb/ExternalFileIngestionInfo.java
        src/main/java/org/forstdb/Filter.java
        src/main/java/org/forstdb/FilterPolicyType.java
        src/main/java/org/forstdb/FileOperationInfo.java
        src/main/java/org/forstdb/FlinkCompactionFilter.java
        src/main/java/org/forstdb/FlinkEnv.java
        src/main/java/org/forstdb/FlushJobInfo.java
        src/main/java/org/forstdb/FlushReason.java
        src/main/java/org/forstdb/FlushOptions.java
        src/main/java/org/forstdb/GetStatus.java
        src/main/java/org/forstdb/HashLinkedListMemTableConfig.java
        src/main/java/org/forstdb/HashSkipListMemTableConfig.java
        src/main/java/org/forstdb/HistogramData.java
        src/main/java/org/forstdb/HistogramType.java
        src/main/java/org/forstdb/Holder.java
        src/main/java/org/forstdb/ImportColumnFamilyOptions.java
        src/main/java/org/forstdb/HyperClockCache.java
        src/main/java/org/forstdb/IndexShorteningMode.java
        src/main/java/org/forstdb/IndexType.java
        src/main/java/org/forstdb/InfoLogLevel.java
        src/main/java/org/forstdb/IngestExternalFileOptions.java
        src/main/java/org/forstdb/LevelMetaData.java
        src/main/java/org/forstdb/ConcurrentTaskLimiter.java
        src/main/java/org/forstdb/ConcurrentTaskLimiterImpl.java
        src/main/java/org/forstdb/KeyMayExist.java
        src/main/java/org/forstdb/LiveFileMetaData.java
        src/main/java/org/forstdb/LogFile.java
        src/main/java/org/forstdb/Logger.java
        src/main/java/org/forstdb/LRUCache.java
        src/main/java/org/forstdb/MemoryUsageType.java
        src/main/java/org/forstdb/MemoryUtil.java
        src/main/java/org/forstdb/MemTableConfig.java
        src/main/java/org/forstdb/MemTableInfo.java
        src/main/java/org/forstdb/MergeOperator.java
        src/main/java/org/forstdb/MutableColumnFamilyOptions.java
        src/main/java/org/forstdb/MutableColumnFamilyOptionsInterface.java
        src/main/java/org/forstdb/MutableDBOptions.java
        src/main/java/org/forstdb/MutableDBOptionsInterface.java
        src/main/java/org/forstdb/MutableOptionKey.java
        src/main/java/org/forstdb/MutableOptionValue.java
        src/main/java/org/forstdb/NativeComparatorWrapper.java
        src/main/java/org/forstdb/NativeLibraryLoader.java
        src/main/java/org/forstdb/OperationStage.java
        src/main/java/org/forstdb/OperationType.java
        src/main/java/org/forstdb/OptimisticTransactionDB.java
        src/main/java/org/forstdb/OptimisticTransactionOptions.java
        src/main/java/org/forstdb/Options.java
        src/main/java/org/forstdb/OptionString.java
        src/main/java/org/forstdb/OptionsUtil.java
        src/main/java/org/forstdb/PersistentCache.java
        src/main/java/org/forstdb/PerfContext.java
        src/main/java/org/forstdb/PerfLevel.java
        src/main/java/org/forstdb/PlainTableConfig.java
        src/main/java/org/forstdb/PrepopulateBlobCache.java
        src/main/java/org/forstdb/Priority.java
        src/main/java/org/forstdb/Range.java
        src/main/java/org/forstdb/RateLimiter.java
        src/main/java/org/forstdb/RateLimiterMode.java
        src/main/java/org/forstdb/ReadOptions.java
        src/main/java/org/forstdb/ReadTier.java
        src/main/java/org/forstdb/RemoveEmptyValueCompactionFilter.java
        src/main/java/org/forstdb/RestoreOptions.java
        src/main/java/org/forstdb/ReusedSynchronisationType.java
        src/main/java/org/forstdb/RocksCallbackObject.java
        src/main/java/org/forstdb/RocksDBException.java
        src/main/java/org/forstdb/RocksDBShutdownHook.java
        src/main/java/org/forstdb/RocksDB.java
        src/main/java/org/forstdb/RocksEnv.java
        src/main/java/org/forstdb/RocksIteratorInterface.java
        src/main/java/org/forstdb/RocksIterator.java
        src/main/java/org/forstdb/RocksMemEnv.java
        src/main/java/org/forstdb/RocksMutableObject.java
        src/main/java/org/forstdb/RocksObject.java
        src/main/java/org/forstdb/SanityLevel.java
        src/main/java/org/forstdb/SizeApproximationFlag.java
        src/main/java/org/forstdb/SkipListMemTableConfig.java
        src/main/java/org/forstdb/Slice.java
        src/main/java/org/forstdb/Snapshot.java
        src/main/java/org/forstdb/SstFileManager.java
        src/main/java/org/forstdb/SstFileMetaData.java
        src/main/java/org/forstdb/SstFileReader.java
        src/main/java/org/forstdb/SstFileReaderIterator.java
        src/main/java/org/forstdb/SstFileWriter.java
        src/main/java/org/forstdb/SstPartitionerFactory.java
        src/main/java/org/forstdb/SstPartitionerFixedPrefixFactory.java
        src/main/java/org/forstdb/StateType.java
        src/main/java/org/forstdb/StatisticsCollectorCallback.java
        src/main/java/org/forstdb/StatisticsCollector.java
        src/main/java/org/forstdb/Statistics.java
        src/main/java/org/forstdb/StatsCollectorInput.java
        src/main/java/org/forstdb/StatsLevel.java
        src/main/java/org/forstdb/Status.java
        src/main/java/org/forstdb/StringAppendOperator.java
        src/main/java/org/forstdb/TableFileCreationBriefInfo.java
        src/main/java/org/forstdb/TableFileCreationInfo.java
        src/main/java/org/forstdb/TableFileCreationReason.java
        src/main/java/org/forstdb/TableFileDeletionInfo.java
        src/main/java/org/forstdb/TableFilter.java
        src/main/java/org/forstdb/TableProperties.java
        src/main/java/org/forstdb/TableFormatConfig.java
        src/main/java/org/forstdb/ThreadType.java
        src/main/java/org/forstdb/ThreadStatus.java
        src/main/java/org/forstdb/TickerType.java
        src/main/java/org/forstdb/TimedEnv.java
        src/main/java/org/forstdb/TraceOptions.java
        src/main/java/org/forstdb/TraceWriter.java
        src/main/java/org/forstdb/TransactionalDB.java
        src/main/java/org/forstdb/TransactionalOptions.java
        src/main/java/org/forstdb/TransactionDB.java
        src/main/java/org/forstdb/TransactionDBOptions.java
        src/main/java/org/forstdb/Transaction.java
        src/main/java/org/forstdb/TransactionLogIterator.java
        src/main/java/org/forstdb/TransactionOptions.java
        src/main/java/org/forstdb/TtlDB.java
        src/main/java/org/forstdb/TxnDBWritePolicy.java
        src/main/java/org/forstdb/VectorMemTableConfig.java
        src/main/java/org/forstdb/WalFileType.java
        src/main/java/org/forstdb/WalFilter.java
        src/main/java/org/forstdb/WalProcessingOption.java
        src/main/java/org/forstdb/WALRecoveryMode.java
        src/main/java/org/forstdb/WBWIRocksIterator.java
        src/main/java/org/forstdb/WriteBatch.java
        src/main/java/org/forstdb/WriteBatchInterface.java
        src/main/java/org/forstdb/WriteBatchWithIndex.java
        src/main/java/org/forstdb/WriteOptions.java
        src/main/java/org/forstdb/WriteBufferManager.java
        src/main/java/org/forstdb/WriteStallCondition.java
        src/main/java/org/forstdb/WriteStallInfo.java
        src/main/java/org/forstdb/util/BufferUtil.java
        src/main/java/org/forstdb/util/ByteUtil.java
        src/main/java/org/forstdb/util/BytewiseComparator.java
        src/main/java/org/forstdb/util/Environment.java
        src/main/java/org/forstdb/util/IntComparator.java
        src/main/java/org/forstdb/util/ReverseBytewiseComparator.java
        src/main/java/org/forstdb/util/SizeUnit.java
        src/main/java/org/forstdb/UInt64AddOperator.java
  src/test/java/org/forstdb/NativeComparatorWrapperTest.java
  src/test/java/org/forstdb/RocksDBExceptionTest.java
  src/test/java/org/forstdb/test/TestableEventListener.java
  src/test/java/org/forstdb/WriteBatchTest.java
  src/test/java/org/forstdb/RocksNativeLibraryResource.java
  src/test/java/org/forstdb/util/CapturingWriteBatchHandler.java
  src/test/java/org/forstdb/util/WriteBatchGetter.java
)

set(JAVA_TEST_CLASSES
  src/test/java/org/forstdb/ConcurrentTaskLimiterTest.java
  src/test/java/org/forstdb/EventListenerTest.java
  src/test/java/org/forstdb/CompactionOptionsTest.java
  src/test/java/org/forstdb/PlatformRandomHelper.java
  src/test/java/org/forstdb/IngestExternalFileOptionsTest.java
  src/test/java/org/forstdb/MutableDBOptionsTest.java
  src/test/java/org/forstdb/WriteOptionsTest.java
  src/test/java/org/forstdb/SstPartitionerTest.java
  src/test/java/org/forstdb/RocksMemEnvTest.java
  src/test/java/org/forstdb/CompactionOptionsUniversalTest.java
  src/test/java/org/forstdb/ClockCacheTest.java
  src/test/java/org/forstdb/BytewiseComparatorRegressionTest.java
  src/test/java/org/forstdb/SnapshotTest.java
  src/test/java/org/forstdb/CompactionJobStatsTest.java
  src/test/java/org/forstdb/MemTableTest.java
  src/test/java/org/forstdb/CompactionFilterFactoryTest.java
  src/test/java/org/forstdb/DefaultEnvTest.java
  src/test/java/org/forstdb/DBOptionsTest.java
  src/test/java/org/forstdb/RocksIteratorTest.java
  src/test/java/org/forstdb/SliceTest.java
  src/test/java/org/forstdb/MultiGetTest.java
  src/test/java/org/forstdb/ComparatorOptionsTest.java
  src/test/java/org/forstdb/NativeLibraryLoaderTest.java
  src/test/java/org/forstdb/StatisticsTest.java
  src/test/java/org/forstdb/WALRecoveryModeTest.java
  src/test/java/org/forstdb/TransactionLogIteratorTest.java
  src/test/java/org/forstdb/ReadOptionsTest.java
  src/test/java/org/forstdb/SecondaryDBTest.java
  src/test/java/org/forstdb/KeyMayExistTest.java
  src/test/java/org/forstdb/BlobOptionsTest.java
  src/test/java/org/forstdb/InfoLogLevelTest.java
  src/test/java/org/forstdb/CompactionPriorityTest.java
  src/test/java/org/forstdb/FlushOptionsTest.java
  src/test/java/org/forstdb/VerifyChecksumsTest.java
  src/test/java/org/forstdb/MultiColumnRegressionTest.java
  src/test/java/org/forstdb/FlushTest.java
  src/test/java/org/forstdb/HyperClockCacheTest.java
  src/test/java/org/forstdb/PutMultiplePartsTest.java
  src/test/java/org/forstdb/StatisticsCollectorTest.java
  src/test/java/org/forstdb/LRUCacheTest.java
  src/test/java/org/forstdb/ColumnFamilyOptionsTest.java
  src/test/java/org/forstdb/TransactionTest.java
  src/test/java/org/forstdb/CompactionOptionsFIFOTest.java
  src/test/java/org/forstdb/BackupEngineOptionsTest.java
  src/test/java/org/forstdb/CheckPointTest.java
  src/test/java/org/forstdb/PlainTableConfigTest.java
  src/test/java/org/forstdb/TransactionDBOptionsTest.java
  src/test/java/org/forstdb/ReadOnlyTest.java
  src/test/java/org/forstdb/EnvOptionsTest.java
  src/test/java/org/forstdb/test/RemoveEmptyValueCompactionFilterFactory.java
  src/test/java/org/forstdb/test/RemoveEmptyValueCompactionFilterFactory.java
  src/test/java/org/forstdb/test/TestableEventListener.java
  src/test/java/org/forstdb/test/RemoveEmptyValueCompactionFilterFactory.java
  src/test/java/org/forstdb/test/TestableEventListener.java
  src/test/java/org/forstdb/test/RocksJunitRunner.java
  src/test/java/org/forstdb/LoggerTest.java
  src/test/java/org/forstdb/FilterTest.java
  src/test/java/org/forstdb/ByteBufferUnsupportedOperationTest.java
  src/test/java/org/forstdb/util/IntComparatorTest.java
  src/test/java/org/forstdb/util/JNIComparatorTest.java
  src/test/java/org/forstdb/util/ByteBufferAllocator.java
  src/test/java/org/forstdb/util/SizeUnitTest.java
  src/test/java/org/forstdb/util/BytewiseComparatorTest.java
  src/test/java/org/forstdb/util/EnvironmentTest.java
  src/test/java/org/forstdb/util/BytewiseComparatorIntTest.java
  src/test/java/org/forstdb/util/DirectByteBufferAllocator.java
  src/test/java/org/forstdb/util/HeapByteBufferAllocator.java
  src/test/java/org/forstdb/util/TestUtil.java
  src/test/java/org/forstdb/util/ReverseBytewiseComparatorIntTest.java
  src/test/java/org/forstdb/Types.java
  src/test/java/org/forstdb/MixedOptionsTest.java
  src/test/java/org/forstdb/CompactRangeOptionsTest.java
  src/test/java/org/forstdb/SstFileWriterTest.java
  src/test/java/org/forstdb/WalFilterTest.java
  src/test/java/org/forstdb/AbstractTransactionTest.java
  src/test/java/org/forstdb/MergeTest.java
  src/test/java/org/forstdb/OptionsTest.java
  src/test/java/org/forstdb/WriteBatchThreadedTest.java
  src/test/java/org/forstdb/MultiGetManyKeysTest.java
  src/test/java/org/forstdb/TimedEnvTest.java
  src/test/java/org/forstdb/CompactionStopStyleTest.java
  src/test/java/org/forstdb/CompactionJobInfoTest.java
  src/test/java/org/forstdb/BlockBasedTableConfigTest.java
  src/test/java/org/forstdb/BuiltinComparatorTest.java
  src/test/java/org/forstdb/RateLimiterTest.java
  src/test/java/org/forstdb/TransactionOptionsTest.java
  src/test/java/org/forstdb/WriteBatchWithIndexTest.java
  src/test/java/org/forstdb/WriteBatchHandlerTest.java
  src/test/java/org/forstdb/OptimisticTransactionDBTest.java
  src/test/java/org/forstdb/OptionsUtilTest.java
  src/test/java/org/forstdb/OptimisticTransactionTest.java
  src/test/java/org/forstdb/MutableColumnFamilyOptionsTest.java
  src/test/java/org/forstdb/CompressionOptionsTest.java
  src/test/java/org/forstdb/ColumnFamilyTest.java
  src/test/java/org/forstdb/SstFileReaderTest.java
  src/test/java/org/forstdb/TransactionDBTest.java
  src/test/java/org/forstdb/RocksDBTest.java
  src/test/java/org/forstdb/MutableOptionsGetSetTest.java
  src/test/java/org/forstdb/OptimisticTransactionOptionsTest.java
  src/test/java/org/forstdb/SstFileManagerTest.java
  src/test/java/org/forstdb/BackupEngineTest.java
  src/test/java/org/forstdb/DirectSliceTest.java
  src/test/java/org/forstdb/StatsCallbackMock.java
  src/test/java/org/forstdb/CompressionTypesTest.java
  src/test/java/org/forstdb/MemoryUtilTest.java
  src/test/java/org/forstdb/TableFilterTest.java
  src/test/java/org/forstdb/TtlDBTest.java
)

set(JAVA_TEST_RUNNING_CLASSES
  org.forstdb.ConcurrentTaskLimiterTest
  org.forstdb.EventListenerTest
  org.forstdb.CompactionOptionsTest
  org.forstdb.IngestExternalFileOptionsTest
  org.forstdb.MutableDBOptionsTest
  org.forstdb.WriteOptionsTest
  org.forstdb.SstPartitionerTest
  org.forstdb.RocksMemEnvTest
  org.forstdb.CompactionOptionsUniversalTest
  org.forstdb.ClockCacheTest
  # org.forstdb.BytewiseComparatorRegressionTest
  org.forstdb.SnapshotTest
  org.forstdb.CompactionJobStatsTest
  org.forstdb.MemTableTest
  org.forstdb.CompactionFilterFactoryTest
  # org.forstdb.DefaultEnvTest
  org.forstdb.DBOptionsTest
  org.forstdb.WriteBatchTest
  org.forstdb.RocksIteratorTest
  org.forstdb.SliceTest
  org.forstdb.MultiGetTest
  org.forstdb.ComparatorOptionsTest
  # org.forstdb.NativeLibraryLoaderTest
  org.forstdb.StatisticsTest
  org.forstdb.WALRecoveryModeTest
  org.forstdb.TransactionLogIteratorTest
  org.forstdb.ReadOptionsTest
  org.forstdb.SecondaryDBTest
  org.forstdb.KeyMayExistTest
  org.forstdb.BlobOptionsTest
  org.forstdb.InfoLogLevelTest
  org.forstdb.CompactionPriorityTest
  org.forstdb.FlushOptionsTest
  org.forstdb.VerifyChecksumsTest
  org.forstdb.MultiColumnRegressionTest
  org.forstdb.FlushTest
  org.forstdb.HyperClockCacheTest
  org.forstdb.PutMultiplePartsTest
  org.forstdb.StatisticsCollectorTest
  org.forstdb.LRUCacheTest
  org.forstdb.ColumnFamilyOptionsTest
  org.forstdb.TransactionTest
  org.forstdb.CompactionOptionsFIFOTest
  org.forstdb.BackupEngineOptionsTest
  org.forstdb.CheckPointTest
  org.forstdb.PlainTableConfigTest
  # org.forstdb.TransactionDBOptionsTest
  org.forstdb.ReadOnlyTest
  org.forstdb.EnvOptionsTest
  org.forstdb.LoggerTest
  org.forstdb.FilterTest
  # org.forstdb.ByteBufferUnsupportedOperationTest
  # org.forstdb.util.IntComparatorTest
  # org.forstdb.util.JNIComparatorTest
  org.forstdb.util.SizeUnitTest
  # org.forstdb.util.BytewiseComparatorTest
  org.forstdb.util.EnvironmentTest
  # org.forstdb.util.BytewiseComparatorIntTest
  # org.forstdb.util.ReverseBytewiseComparatorIntTest
  org.forstdb.MixedOptionsTest
  org.forstdb.CompactRangeOptionsTest
  # org.forstdb.SstFileWriterTest
  org.forstdb.WalFilterTest
  # org.forstdb.AbstractTransactionTest
  org.forstdb.MergeTest
  org.forstdb.OptionsTest
  org.forstdb.WriteBatchThreadedTest
  org.forstdb.MultiGetManyKeysTest
  org.forstdb.TimedEnvTest
  org.forstdb.CompactionStopStyleTest
  org.forstdb.CompactionJobInfoTest
  org.forstdb.BlockBasedTableConfigTest
  org.forstdb.BuiltinComparatorTest
  org.forstdb.RateLimiterTest
  # org.forstdb.TransactionOptionsTest
  org.forstdb.WriteBatchWithIndexTest
  org.forstdb.WriteBatchHandlerTest
  org.forstdb.OptimisticTransactionDBTest
  org.forstdb.OptionsUtilTest
  org.forstdb.OptimisticTransactionTest
  org.forstdb.MutableColumnFamilyOptionsTest
  org.forstdb.CompressionOptionsTest
  org.forstdb.ColumnFamilyTest
  org.forstdb.SstFileReaderTest
  org.forstdb.TransactionDBTest
  org.forstdb.RocksDBTest
  org.forstdb.MutableOptionsGetSetTest
  # org.forstdb.OptimisticTransactionOptionsTest
  org.forstdb.SstFileManagerTest
  org.forstdb.BackupEngineTest
  org.forstdb.DirectSliceTest
  org.forstdb.CompressionTypesTest
  org.forstdb.MemoryUtilTest
  org.forstdb.TableFilterTest
  org.forstdb.TtlDBTest
)

include(FindJava)
include(UseJava)
find_package(JNI)

include_directories(${JNI_INCLUDE_DIRS})
include_directories(${PROJECT_SOURCE_DIR}/java)

set(JAVA_TEST_LIBDIR ${PROJECT_SOURCE_DIR}/java/test-libs)
set(JAVA_TMP_JAR ${JAVA_TEST_LIBDIR}/tmp.jar)
set(JAVA_JUNIT_JAR ${JAVA_TEST_LIBDIR}/junit-${JAVA_JUNIT_VERSION}.jar)
set(JAVA_HAMCR_JAR ${JAVA_TEST_LIBDIR}/hamcrest-${JAVA_HAMCR_VERSION}.jar)
set(JAVA_MOCKITO_JAR ${JAVA_TEST_LIBDIR}/mockito-all-${JAVA_MOCKITO_VERSION}.jar)
set(JAVA_CGLIB_JAR ${JAVA_TEST_LIBDIR}/cglib-${JAVA_CGLIB_VERSION}.jar)
set(JAVA_ASSERTJ_JAR ${JAVA_TEST_LIBDIR}/assertj-core-${JAVA_ASSERTJ_VERSION}.jar)
set(JAVA_TESTCLASSPATH ${JAVA_JUNIT_JAR} ${JAVA_HAMCR_JAR} ${JAVA_MOCKITO_JAR} ${JAVA_CGLIB_JAR} ${JAVA_ASSERTJ_JAR})
message("CMAKE_SYSTEM_NAME: ${CMAKE_SYSTEM_NAME}")
message("MINGW: ${MINGW}")

if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  set(JAVA_RUN_TESTCLASSPATH ${JAVA_JUNIT_JAR}$<SEMICOLON>${JAVA_HAMCR_JAR}$<SEMICOLON>${JAVA_MOCKITO_JAR}$<SEMICOLON>${JAVA_CGLIB_JAR}$<SEMICOLON>${JAVA_ASSERTJ_JAR})
else()
  set(JAVA_RUN_TESTCLASSPATH ${JAVA_JUNIT_JAR}:${JAVA_HAMCR_JAR}:${JAVA_MOCKITO_JAR}:${JAVA_CGLIB_JAR}:${JAVA_ASSERTJ_JAR})
endif()

set(JNI_OUTPUT_DIR ${PROJECT_SOURCE_DIR}/java/include)
file(MAKE_DIRECTORY ${JNI_OUTPUT_DIR})

if(${Java_VERSION_MINOR} VERSION_LESS_EQUAL "7" AND ${Java_VERSION_MAJOR} STREQUAL "1")
  message(FATAL_ERROR "Detected Java 7 or older (${Java_VERSION_STRING}), minimum required version in now Java 8")
endif()

if(${Java_VERSION_MAJOR} VERSION_GREATER_EQUAL "10" AND ${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Java 10 and newer don't have javah, but the alternative GENERATE_NATIVE_HEADERS requires CMake 3.11.4 or newer
  message(FATAL_ERROR "Detected Java 10 or newer (${Java_VERSION_STRING}), to build with CMake please upgrade CMake to 3.11.4 or newer")

elseif(${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Old CMake
  message("Using an old CMAKE (${CMAKE_VERSION}) - JNI headers generated in separate step")
  add_jar(
      rocksdbjni_classes
      SOURCES ${JAVA_MAIN_CLASSES}
  )

else ()
  # Java 1.8 or newer prepare the JAR...
  message("Preparing Jar for JDK ${Java_VERSION_STRING}")
  message("JAVA_TESTCLASSPATH=${JAVA_TESTCLASSPATH}")
  add_jar(
      rocksdbjni_classes
      SOURCES ${JAVA_MAIN_CLASSES}
      INCLUDE_JARS ${ROCKSDBJNI_CLASSES_JAR_FILE} ${JAVA_TESTCLASSPATH}
      GENERATE_NATIVE_HEADERS rocksdbjni_headers DESTINATION ${JNI_OUTPUT_DIR}
  )

endif()

add_jar(
    rocksdbjni_test_classes
    SOURCES
    ${JAVA_MAIN_CLASSES}
    ${JAVA_TEST_CLASSES}
    INCLUDE_JARS ${JAVA_TESTCLASSPATH}
    GENERATE_NATIVE_HEADERS rocksdbjni_test_headers DESTINATION ${JNI_OUTPUT_DIR}
)

if(NOT EXISTS ${PROJECT_SOURCE_DIR}/java/classes)
  file(MAKE_DIRECTORY ${PROJECT_SOURCE_DIR}/java/classes)
endif()

if(NOT EXISTS ${JAVA_TEST_LIBDIR})
  file(MAKE_DIRECTORY mkdir ${JAVA_TEST_LIBDIR})
endif()

if (DEFINED CUSTOM_DEPS_URL)
  set(DEPS_URL ${CUSTOM_DEPS_URL}/)
else ()
  # Using a Facebook AWS account for S3 storage. (maven.org has a history
  # of failing in Travis builds.)
  set(DEPS_URL "https://rocksdb-deps.s3-us-west-2.amazonaws.com/jars")
endif()

if(NOT EXISTS ${JAVA_JUNIT_JAR})
  message("Downloading ${JAVA_JUNIT_JAR}")
  file(DOWNLOAD ${DEPS_URL}/junit-${JAVA_JUNIT_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_JUNIT_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_JUNIT_JAR})
endif()
if(NOT EXISTS ${JAVA_HAMCR_JAR})
  message("Downloading ${JAVA_HAMCR_JAR}")
  file(DOWNLOAD ${DEPS_URL}/hamcrest-${JAVA_HAMCR_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_HAMCR_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_HAMCR_JAR})
endif()
if(NOT EXISTS ${JAVA_MOCKITO_JAR})
  message("Downloading ${JAVA_MOCKITO_JAR}")
  file(DOWNLOAD ${DEPS_URL}/mockito-all-${JAVA_MOCKITO_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_MOCKITO_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_MOCKITO_JAR})
endif()
if(NOT EXISTS ${JAVA_CGLIB_JAR})
  message("Downloading ${JAVA_CGLIB_JAR}")
  file(DOWNLOAD ${DEPS_URL}/cglib-${JAVA_CGLIB_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_CGLIB_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_CGLIB_JAR})
endif()
if(NOT EXISTS ${JAVA_ASSERTJ_JAR})
  message("Downloading ${JAVA_ASSERTJ_JAR}")
  file(DOWNLOAD ${DEPS_URL}/assertj-core-${JAVA_ASSERTJ_VERSION}.jar ${JAVA_TMP_JAR} STATUS downloadStatus)
  list(GET downloadStatus 0 error_code)
  list(GET downloadStatus 1 error_message)
  if(NOT error_code EQUAL 0)
    message(FATAL_ERROR "Failed downloading ${JAVA_ASSERTJ_JAR}: ${error_message}")
  endif()
  file(RENAME ${JAVA_TMP_JAR} ${JAVA_ASSERTJ_JAR})
endif()

if(${CMAKE_VERSION} VERSION_LESS "3.11.4")
  # Old CMake ONLY generate JNI headers, otherwise JNI is handled in add_jar step above
  message("Preparing JNI headers for old CMake (${CMAKE_VERSION})")
  set(NATIVE_JAVA_CLASSES
          org.forstdb.AbstractCompactionFilter
          org.forstdb.AbstractCompactionFilterFactory
          org.forstdb.AbstractComparator
          org.forstdb.AbstractEventListener
          org.forstdb.AbstractImmutableNativeReference
          org.forstdb.AbstractNativeReference
          org.forstdb.AbstractRocksIterator
          org.forstdb.AbstractSlice
          org.forstdb.AbstractTableFilter
          org.forstdb.AbstractTraceWriter
          org.forstdb.AbstractTransactionNotifier
          org.forstdb.AbstractWalFilter
          org.forstdb.BackupEngineOptions
          org.forstdb.BackupEngine
          org.forstdb.BlockBasedTableConfig
          org.forstdb.BloomFilter
          org.forstdb.CassandraCompactionFilter
          org.forstdb.CassandraValueMergeOperator
          org.forstdb.Checkpoint
          org.forstdb.ClockCache
          org.forstdb.Cache
          org.forstdb.ColumnFamilyHandle
          org.forstdb.ColumnFamilyOptions
          org.forstdb.CompactionJobInfo
          org.forstdb.CompactionJobStats
          org.forstdb.CompactionOptions
          org.forstdb.CompactionOptionsFIFO
          org.forstdb.CompactionOptionsUniversal
          org.forstdb.CompactRangeOptions
          org.forstdb.ComparatorOptions
          org.forstdb.CompressionOptions
          org.forstdb.ConcurrentTaskLimiterImpl
          org.forstdb.ConfigOptions
          org.forstdb.DBOptions
          org.forstdb.DirectSlice
          org.forstdb.Env
          org.forstdb.EnvFlinkTestSuite
          org.forstdb.EnvOptions
          org.forstdb.Filter
          org.forstdb.FlinkCompactionFilter
          org.forstdb.FlinkEnv
          org.forstdb.FlushOptions
          org.forstdb.HashLinkedListMemTableConfig
          org.forstdb.HashSkipListMemTableConfig
          org.forstdb.HyperClockCache
          org.forstdb.IngestExternalFileOptions
          org.forstdb.Logger
          org.forstdb.LRUCache
          org.forstdb.MemoryUtil
          org.forstdb.MemTableConfig
          org.forstdb.NativeComparatorWrapper
          org.forstdb.NativeLibraryLoader
          org.forstdb.OptimisticTransactionDB
          org.forstdb.OptimisticTransactionOptions
          org.forstdb.Options
          org.forstdb.OptionsUtil
          org.forstdb.PersistentCache
          org.forstdb.PlainTableConfig
          org.forstdb.RateLimiter
          org.forstdb.ReadOptions
          org.forstdb.RemoveEmptyValueCompactionFilter
          org.forstdb.RestoreOptions
          org.forstdb.RocksCallbackObject
          org.forstdb.RocksDB
          org.forstdb.RocksEnv
          org.forstdb.RocksIterator
          org.forstdb.RocksIteratorInterface
          org.forstdb.RocksMemEnv
          org.forstdb.RocksMutableObject
          org.forstdb.RocksObject
          org.forstdb.SkipListMemTableConfig
          org.forstdb.Slice
          org.forstdb.Snapshot
          org.forstdb.SstFileManager
          org.forstdb.SstFileWriter
          org.forstdb.SstFileReader
          org.forstdb.SstFileReaderIterator
          org.forstdb.SstPartitionerFactory
          org.forstdb.SstPartitionerFixedPrefixFactory
          org.forstdb.Statistics
          org.forstdb.StringAppendOperator
          org.forstdb.TableFormatConfig
          org.forstdb.ThreadStatus
          org.forstdb.TimedEnv
          org.forstdb.Transaction
          org.forstdb.TransactionDB
          org.forstdb.TransactionDBOptions
          org.forstdb.TransactionLogIterator
          org.forstdb.TransactionOptions
          org.forstdb.TtlDB
          org.forstdb.UInt64AddOperator
          org.forstdb.VectorMemTableConfig
          org.forstdb.WBWIRocksIterator
          org.forstdb.WriteBatch
          org.forstdb.WriteBatch.Handler
          org.forstdb.WriteBatchInterface
          org.forstdb.WriteBatchWithIndex
          org.forstdb.WriteOptions
          org.forstdb.NativeComparatorWrapperTest
          org.forstdb.RocksDBExceptionTest
          org.forstdb.SnapshotTest
          org.forstdb.WriteBatchTest
          org.forstdb.WriteBatchTestInternalHelper
          org.forstdb.WriteBufferManager
          org.forstdb.test.TestableEventListener
  )

  create_javah(
    TARGET rocksdbjni_headers
    CLASSES ${NATIVE_JAVA_CLASSES}
    CLASSPATH rocksdbjni_classes ${JAVA_TESTCLASSPATH}
    OUTPUT_DIR ${JNI_OUTPUT_DIR}
  )
endif()

if(NOT MSVC)
  set_property(TARGET ${ROCKSDB_STATIC_LIB} PROPERTY POSITION_INDEPENDENT_CODE ON)
endif()

set(ROCKSDBJNI_STATIC_LIB rocksdbjni${ARTIFACT_SUFFIX})
if(MINGW)
  # Build mingw only as staic library.
  add_library(${ROCKSDBJNI_STATIC_LIB} ${JNI_NATIVE_SOURCES})
else()
  add_library(${ROCKSDBJNI_STATIC_LIB} SHARED ${JNI_NATIVE_SOURCES})
endif()
add_dependencies(${ROCKSDBJNI_STATIC_LIB} rocksdbjni_headers)
target_link_libraries(${ROCKSDBJNI_STATIC_LIB} ${ROCKSDB_STATIC_LIB} ${ROCKSDB_LIB})

if(NOT MINGW)
  set(ROCKSDBJNI_SHARED_LIB rocksdbjni-shared${ARTIFACT_SUFFIX})
  add_library(${ROCKSDBJNI_SHARED_LIB} SHARED ${JNI_NATIVE_SOURCES})
  add_dependencies(${ROCKSDBJNI_SHARED_LIB} rocksdbjni_headers)
  target_link_libraries(${ROCKSDBJNI_SHARED_LIB} ${ROCKSDB_STATIC_LIB} ${ROCKSDB_LIB})

  set_target_properties(
    ${ROCKSDBJNI_SHARED_LIB}
    PROPERTIES
    COMPILE_PDB_OUTPUT_DIRECTORY ${CMAKE_CFG_INTDIR}
    COMPILE_PDB_NAME ${ROCKSDBJNI_STATIC_LIB}.pdb
  )
endif()

enable_testing()
get_target_property(ROCKSDBJNI_CLASSES_TEST_JAR_FILE rocksdbjni_test_classes JAR_FILE)
foreach (CLAZZ ${JAVA_TEST_RUNNING_CLASSES})
  if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    add_test(
      NAME jtest_${CLAZZ}
      COMMAND ${Java_JAVA_EXECUTABLE} ${JVMARGS} -ea -Xcheck:jni -Djava.library.path=${PROJECT_BINARY_DIR}/java/${CMAKE_BUILD_TYPE} -classpath ${JAVA_RUN_TESTCLASSPATH}$<SEMICOLON>${ROCKSDBJNI_CLASSES_TEST_JAR_FILE} org.forstdb.test.RocksJunitRunner ${CLAZZ}
    )
  else()
    add_test(
      NAME jtest_${CLAZZ}
      COMMAND ${Java_JAVA_EXECUTABLE} ${JVMARGS} -ea -Xcheck:jni -Djava.library.path=${PROJECT_BINARY_DIR}/java -classpath ${JAVA_RUN_TESTCLASSPATH}:${ROCKSDBJNI_CLASSES_TEST_JAR_FILE} org.forstdb.test.RocksJunitRunner ${CLAZZ}
    )
  endif()
endforeach(CLAZZ)