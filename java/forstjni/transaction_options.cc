// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++
// for ROCKSDB_NAMESPACE::TransactionOptions.

#include <jni.h>

#include "include/org_forstdb_TransactionOptions.h"
#include "rocksdb/utilities/transaction_db.h"
#include "forstjni/cplusplus_to_java_convert.h"

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    newTransactionOptions
 * Signature: ()J
 */
jlong Java_org_forstdb_TransactionOptions_newTransactionOptions(
    JNIEnv* /*env*/, jclass /*jcls*/) {
  auto* opts = new ROCKSDB_NAMESPACE::TransactionOptions();
  return GET_CPLUSPLUS_POINTER(opts);
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    isSetSnapshot
 * Signature: (J)Z
 */
jboolean Java_org_forstdb_TransactionOptions_isSetSnapshot(JNIEnv* /*env*/,
                                                           jobject /*jobj*/,
                                                           jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  return opts->set_snapshot;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    setSetSnapshot
 * Signature: (JZ)V
 */
void Java_org_forstdb_TransactionOptions_setSetSnapshot(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle, jboolean jset_snapshot) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  opts->set_snapshot = jset_snapshot;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    isDeadlockDetect
 * Signature: (J)Z
 */
jboolean Java_org_forstdb_TransactionOptions_isDeadlockDetect(JNIEnv* /*env*/,
                                                              jobject /*jobj*/,
                                                              jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  return opts->deadlock_detect;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    setDeadlockDetect
 * Signature: (JZ)V
 */
void Java_org_forstdb_TransactionOptions_setDeadlockDetect(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jboolean jdeadlock_detect) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  opts->deadlock_detect = jdeadlock_detect;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    getLockTimeout
 * Signature: (J)J
 */
jlong Java_org_forstdb_TransactionOptions_getLockTimeout(JNIEnv* /*env*/,
                                                         jobject /*jobj*/,
                                                         jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  return opts->lock_timeout;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    setLockTimeout
 * Signature: (JJ)V
 */
void Java_org_forstdb_TransactionOptions_setLockTimeout(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jhandle,
                                                        jlong jlock_timeout) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  opts->lock_timeout = jlock_timeout;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    getExpiration
 * Signature: (J)J
 */
jlong Java_org_forstdb_TransactionOptions_getExpiration(JNIEnv* /*env*/,
                                                        jobject /*jobj*/,
                                                        jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  return opts->expiration;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    setExpiration
 * Signature: (JJ)V
 */
void Java_org_forstdb_TransactionOptions_setExpiration(JNIEnv* /*env*/,
                                                       jobject /*jobj*/,
                                                       jlong jhandle,
                                                       jlong jexpiration) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  opts->expiration = jexpiration;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    getDeadlockDetectDepth
 * Signature: (J)J
 */
jlong Java_org_forstdb_TransactionOptions_getDeadlockDetectDepth(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  return opts->deadlock_detect_depth;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    setDeadlockDetectDepth
 * Signature: (JJ)V
 */
void Java_org_forstdb_TransactionOptions_setDeadlockDetectDepth(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jdeadlock_detect_depth) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  opts->deadlock_detect_depth = jdeadlock_detect_depth;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    getMaxWriteBatchSize
 * Signature: (J)J
 */
jlong Java_org_forstdb_TransactionOptions_getMaxWriteBatchSize(JNIEnv* /*env*/,
                                                               jobject /*jobj*/,
                                                               jlong jhandle) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  return opts->max_write_batch_size;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    setMaxWriteBatchSize
 * Signature: (JJ)V
 */
void Java_org_forstdb_TransactionOptions_setMaxWriteBatchSize(
    JNIEnv* /*env*/, jobject /*jobj*/, jlong jhandle,
    jlong jmax_write_batch_size) {
  auto* opts =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
  opts->max_write_batch_size = jmax_write_batch_size;
}

/*
 * Class:     org_forstdb_TransactionOptions
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_forstdb_TransactionOptions_disposeInternal(JNIEnv* /*env*/,
                                                         jobject /*jobj*/,
                                                         jlong jhandle) {
  delete reinterpret_cast<ROCKSDB_NAMESPACE::TransactionOptions*>(jhandle);
}
