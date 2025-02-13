// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Iterator methods from Java side.

#include "rocksdb/transaction_log.h"

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include "include/org_forstdb_TransactionLogIterator.h"
#include "forstjni/portal.h"

/*
 * Class:     org_forstdb_TransactionLogIterator
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_forstdb_TransactionLogIterator_disposeInternal(JNIEnv* /*env*/,
                                                             jobject /*jobj*/,
                                                             jlong handle) {
  delete reinterpret_cast<ROCKSDB_NAMESPACE::TransactionLogIterator*>(handle);
}

/*
 * Class:     org_forstdb_TransactionLogIterator
 * Method:    isValid
 * Signature: (J)Z
 */
jboolean Java_org_forstdb_TransactionLogIterator_isValid(JNIEnv* /*env*/,
                                                         jobject /*jobj*/,
                                                         jlong handle) {
  return reinterpret_cast<ROCKSDB_NAMESPACE::TransactionLogIterator*>(handle)
      ->Valid();
}

/*
 * Class:     org_forstdb_TransactionLogIterator
 * Method:    next
 * Signature: (J)V
 */
void Java_org_forstdb_TransactionLogIterator_next(JNIEnv* /*env*/,
                                                  jobject /*jobj*/,
                                                  jlong handle) {
  reinterpret_cast<ROCKSDB_NAMESPACE::TransactionLogIterator*>(handle)->Next();
}

/*
 * Class:     org_forstdb_TransactionLogIterator
 * Method:    status
 * Signature: (J)V
 */
void Java_org_forstdb_TransactionLogIterator_status(JNIEnv* env,
                                                    jobject /*jobj*/,
                                                    jlong handle) {
  ROCKSDB_NAMESPACE::Status s =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionLogIterator*>(handle)
          ->status();
  if (!s.ok()) {
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, s);
  }
}

/*
 * Class:     org_forstdb_TransactionLogIterator
 * Method:    getBatch
 * Signature: (J)Lorg/forstdb/TransactionLogIterator$BatchResult
 */
jobject Java_org_forstdb_TransactionLogIterator_getBatch(JNIEnv* env,
                                                         jobject /*jobj*/,
                                                         jlong handle) {
  ROCKSDB_NAMESPACE::BatchResult batch_result =
      reinterpret_cast<ROCKSDB_NAMESPACE::TransactionLogIterator*>(handle)
          ->GetBatch();
  return ROCKSDB_NAMESPACE::BatchResultJni::construct(env, batch_result);
}
