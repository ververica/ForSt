// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::WriteBatch methods from Java side.
#include "rocksdb/write_batch.h"

#include <memory>

#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "include/org_forstdb_WriteBatch.h"
#include "include/org_forstdb_WriteBatch_Handler.h"
#include "logging/logging.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "rocksdb/write_buffer_manager.h"
#include "forstjni/cplusplus_to_java_convert.h"
#include "forstjni/portal.h"
#include "forstjni/writebatchhandlerjnicallback.h"
#include "table/scoped_arena_iterator.h"

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    newWriteBatch
 * Signature: (I)J
 */
jlong Java_org_forstdb_WriteBatch_newWriteBatch__I(JNIEnv* /*env*/,
                                                   jclass /*jcls*/,
                                                   jint jreserved_bytes) {
  auto* wb =
      new ROCKSDB_NAMESPACE::WriteBatch(static_cast<size_t>(jreserved_bytes));
  return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    newWriteBatch
 * Signature: ([BI)J
 */
jlong Java_org_forstdb_WriteBatch_newWriteBatch___3BI(JNIEnv* env,
                                                      jclass /*jcls*/,
                                                      jbyteArray jserialized,
                                                      jint jserialized_length) {
  jboolean has_exception = JNI_FALSE;
  std::string serialized = ROCKSDB_NAMESPACE::JniUtil::byteString<std::string>(
      env, jserialized, jserialized_length,
      [](const char* str, const size_t len) { return std::string(str, len); },
      &has_exception);
  if (has_exception == JNI_TRUE) {
    // exception occurred
    return 0;
  }

  auto* wb = new ROCKSDB_NAMESPACE::WriteBatch(serialized);
  return GET_CPLUSPLUS_POINTER(wb);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    count0
 * Signature: (J)I
 */
jint Java_org_forstdb_WriteBatch_count0(JNIEnv* /*env*/, jobject /*jobj*/,
                                        jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return static_cast<jint>(wb->Count());
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    clear0
 * Signature: (J)V
 */
void Java_org_forstdb_WriteBatch_clear0(JNIEnv* /*env*/, jobject /*jobj*/,
                                        jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  wb->Clear();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    setSavePoint0
 * Signature: (J)V
 */
void Java_org_forstdb_WriteBatch_setSavePoint0(JNIEnv* /*env*/,
                                               jobject /*jobj*/,
                                               jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  wb->SetSavePoint();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    rollbackToSavePoint0
 * Signature: (J)V
 */
void Java_org_forstdb_WriteBatch_rollbackToSavePoint0(JNIEnv* env,
                                                      jobject /*jobj*/,
                                                      jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto s = wb->RollbackToSavePoint();

  if (s.ok()) {
    return;
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    popSavePoint
 * Signature: (J)V
 */
void Java_org_forstdb_WriteBatch_popSavePoint(JNIEnv* env, jobject /*jobj*/,
                                              jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto s = wb->PopSavePoint();

  if (s.ok()) {
    return;
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    setMaxBytes
 * Signature: (JJ)V
 */
void Java_org_forstdb_WriteBatch_setMaxBytes(JNIEnv* /*env*/, jobject /*jobj*/,
                                             jlong jwb_handle,
                                             jlong jmax_bytes) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  wb->SetMaxBytes(static_cast<size_t>(jmax_bytes));
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    put
 * Signature: (J[BI[BI)V
 */
void Java_org_forstdb_WriteBatch_put__J_3BI_3BI(JNIEnv* env, jobject jobj,
                                                jlong jwb_handle,
                                                jbyteArray jkey, jint jkey_len,
                                                jbyteArray jentry_value,
                                                jint jentry_value_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto put = [&wb](ROCKSDB_NAMESPACE::Slice key,
                   ROCKSDB_NAMESPACE::Slice value) {
    return wb->Put(key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(put, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    put
 * Signature: (J[BI[BIJ)V
 */
void Java_org_forstdb_WriteBatch_put__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto put = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice key,
                               ROCKSDB_NAMESPACE::Slice value) {
    return wb->Put(cf_handle, key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(put, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    putDirect
 * Signature: (JLjava/nio/ByteBuffer;IILjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_forstdb_WriteBatch_putDirect(JNIEnv* env, jobject /*jobj*/,
                                           jlong jwb_handle, jobject jkey,
                                           jint jkey_offset, jint jkey_len,
                                           jobject jval, jint jval_offset,
                                           jint jval_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto put = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice& key,
                               ROCKSDB_NAMESPACE::Slice& value) {
    if (cf_handle == nullptr) {
      wb->Put(key, value);
    } else {
      wb->Put(cf_handle, key, value);
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::kv_op_direct(
      put, env, jkey, jkey_offset, jkey_len, jval, jval_offset, jval_len);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    merge
 * Signature: (J[BI[BI)V
 */
void Java_org_forstdb_WriteBatch_merge__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto merge = [&wb](ROCKSDB_NAMESPACE::Slice key,
                     ROCKSDB_NAMESPACE::Slice value) {
    return wb->Merge(key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    merge
 * Signature: (J[BI[BIJ)V
 */
void Java_org_forstdb_WriteBatch_merge__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jkey, jint jkey_len,
    jbyteArray jentry_value, jint jentry_value_len, jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto merge = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice key,
                                 ROCKSDB_NAMESPACE::Slice value) {
    return wb->Merge(cf_handle, key, value);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(merge, env, jobj, jkey, jkey_len,
                                        jentry_value, jentry_value_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    delete
 * Signature: (J[BI)V
 */
void Java_org_forstdb_WriteBatch_delete__J_3BI(JNIEnv* env, jobject jobj,
                                               jlong jwb_handle,
                                               jbyteArray jkey, jint jkey_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto remove = [&wb](ROCKSDB_NAMESPACE::Slice key) { return wb->Delete(key); };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    delete
 * Signature: (J[BIJ)V
 */
void Java_org_forstdb_WriteBatch_delete__J_3BIJ(JNIEnv* env, jobject jobj,
                                                jlong jwb_handle,
                                                jbyteArray jkey, jint jkey_len,
                                                jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto remove = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice key) {
    return wb->Delete(cf_handle, key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(remove, env, jobj, jkey, jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    singleDelete
 * Signature: (J[BI)V
 */
void Java_org_forstdb_WriteBatch_singleDelete__J_3BI(JNIEnv* env, jobject jobj,
                                                     jlong jwb_handle,
                                                     jbyteArray jkey,
                                                     jint jkey_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto single_delete = [&wb](ROCKSDB_NAMESPACE::Slice key) {
    return wb->SingleDelete(key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(single_delete, env, jobj, jkey,
                                       jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    singleDelete
 * Signature: (J[BIJ)V
 */
void Java_org_forstdb_WriteBatch_singleDelete__J_3BIJ(JNIEnv* env, jobject jobj,
                                                      jlong jwb_handle,
                                                      jbyteArray jkey,
                                                      jint jkey_len,
                                                      jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto single_delete = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice key) {
    return wb->SingleDelete(cf_handle, key);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(single_delete, env, jobj, jkey,
                                       jkey_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    deleteDirect
 * Signature: (JLjava/nio/ByteBuffer;IIJ)V
 */
void Java_org_forstdb_WriteBatch_deleteDirect(JNIEnv* env, jobject /*jobj*/,
                                              jlong jwb_handle, jobject jkey,
                                              jint jkey_offset, jint jkey_len,
                                              jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  auto remove = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice& key) {
    if (cf_handle == nullptr) {
      wb->Delete(key);
    } else {
      wb->Delete(cf_handle, key);
    }
  };
  ROCKSDB_NAMESPACE::JniUtil::k_op_direct(remove, env, jkey, jkey_offset,
                                          jkey_len);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    deleteRange
 * Signature: (J[BI[BI)V
 */
void Java_org_forstdb_WriteBatch_deleteRange__J_3BI_3BI(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto deleteRange = [&wb](ROCKSDB_NAMESPACE::Slice beginKey,
                           ROCKSDB_NAMESPACE::Slice endKey) {
    return wb->DeleteRange(beginKey, endKey);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    deleteRange
 * Signature: (J[BI[BIJ)V
 */
void Java_org_forstdb_WriteBatch_deleteRange__J_3BI_3BIJ(
    JNIEnv* env, jobject jobj, jlong jwb_handle, jbyteArray jbegin_key,
    jint jbegin_key_len, jbyteArray jend_key, jint jend_key_len,
    jlong jcf_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto* cf_handle =
      reinterpret_cast<ROCKSDB_NAMESPACE::ColumnFamilyHandle*>(jcf_handle);
  assert(cf_handle != nullptr);
  auto deleteRange = [&wb, &cf_handle](ROCKSDB_NAMESPACE::Slice beginKey,
                                       ROCKSDB_NAMESPACE::Slice endKey) {
    return wb->DeleteRange(cf_handle, beginKey, endKey);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::kv_op(deleteRange, env, jobj, jbegin_key,
                                        jbegin_key_len, jend_key, jend_key_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    putLogData
 * Signature: (J[BI)V
 */
void Java_org_forstdb_WriteBatch_putLogData(JNIEnv* env, jobject jobj,
                                            jlong jwb_handle, jbyteArray jblob,
                                            jint jblob_len) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);
  auto putLogData = [&wb](ROCKSDB_NAMESPACE::Slice blob) {
    return wb->PutLogData(blob);
  };
  std::unique_ptr<ROCKSDB_NAMESPACE::Status> status =
      ROCKSDB_NAMESPACE::JniUtil::k_op(putLogData, env, jobj, jblob, jblob_len);
  if (status != nullptr && !status->ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
  }
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    iterate
 * Signature: (JJ)V
 */
void Java_org_forstdb_WriteBatch_iterate(JNIEnv* env, jobject /*jobj*/,
                                         jlong jwb_handle,
                                         jlong handlerHandle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  ROCKSDB_NAMESPACE::Status s = wb->Iterate(
      reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatchHandlerJniCallback*>(
          handlerHandle));

  if (s.ok()) {
    return;
  }
  ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    data
 * Signature: (J)[B
 */
jbyteArray Java_org_forstdb_WriteBatch_data(JNIEnv* env, jobject /*jobj*/,
                                            jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto data = wb->Data();
  return ROCKSDB_NAMESPACE::JniUtil::copyBytes(env, data);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    getDataSize
 * Signature: (J)J
 */
jlong Java_org_forstdb_WriteBatch_getDataSize(JNIEnv* /*env*/, jobject /*jobj*/,
                                              jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto data_size = wb->GetDataSize();
  return static_cast<jlong>(data_size);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    hasPut
 * Signature: (J)Z
 */
jboolean Java_org_forstdb_WriteBatch_hasPut(JNIEnv* /*env*/, jobject /*jobj*/,
                                            jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasPut();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    hasDelete
 * Signature: (J)Z
 */
jboolean Java_org_forstdb_WriteBatch_hasDelete(JNIEnv* /*env*/,
                                               jobject /*jobj*/,
                                               jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasDelete();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    hasSingleDelete
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_forstdb_WriteBatch_hasSingleDelete(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasSingleDelete();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    hasDeleteRange
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_forstdb_WriteBatch_hasDeleteRange(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasDeleteRange();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    hasMerge
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_forstdb_WriteBatch_hasMerge(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasMerge();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    hasBeginPrepare
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_forstdb_WriteBatch_hasBeginPrepare(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasBeginPrepare();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    hasEndPrepare
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_forstdb_WriteBatch_hasEndPrepare(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasEndPrepare();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    hasCommit
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_forstdb_WriteBatch_hasCommit(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasCommit();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    hasRollback
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_org_forstdb_WriteBatch_hasRollback(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  return wb->HasRollback();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    markWalTerminationPoint
 * Signature: (J)V
 */
void Java_org_forstdb_WriteBatch_markWalTerminationPoint(JNIEnv* /*env*/,
                                                         jobject /*jobj*/,
                                                         jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  wb->MarkWalTerminationPoint();
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    getWalTerminationPoint
 * Signature: (J)Lorg/forstdb/WriteBatch/SavePoint;
 */
jobject Java_org_forstdb_WriteBatch_getWalTerminationPoint(JNIEnv* env,
                                                           jobject /*jobj*/,
                                                           jlong jwb_handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(jwb_handle);
  assert(wb != nullptr);

  auto save_point = wb->GetWalTerminationPoint();
  return ROCKSDB_NAMESPACE::WriteBatchSavePointJni::construct(env, save_point);
}

/*
 * Class:     org_forstdb_WriteBatch
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_forstdb_WriteBatch_disposeInternal(JNIEnv* /*env*/,
                                                 jobject /*jobj*/,
                                                 jlong handle) {
  auto* wb = reinterpret_cast<ROCKSDB_NAMESPACE::WriteBatch*>(handle);
  assert(wb != nullptr);
  delete wb;
}

/*
 * Class:     org_forstdb_WriteBatch_Handler
 * Method:    createNewHandler0
 * Signature: ()J
 */
jlong Java_org_forstdb_WriteBatch_00024Handler_createNewHandler0(JNIEnv* env,
                                                                 jobject jobj) {
  auto* wbjnic = new ROCKSDB_NAMESPACE::WriteBatchHandlerJniCallback(env, jobj);
  return GET_CPLUSPLUS_POINTER(wbjnic);
}
