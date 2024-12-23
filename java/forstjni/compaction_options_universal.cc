// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ for
// ROCKSDB_NAMESPACE::CompactionOptionsUniversal.

#include <jni.h>

#include "include/org_forstdb_CompactionOptionsUniversal.h"
#include "rocksdb/advanced_options.h"
#include "forstjni/cplusplus_to_java_convert.h"
#include "forstjni/portal.h"

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    newCompactionOptionsUniversal
 * Signature: ()J
 */
jlong Java_org_forstdb_CompactionOptionsUniversal_newCompactionOptionsUniversal(
    JNIEnv*, jclass) {
  const auto* opt = new ROCKSDB_NAMESPACE::CompactionOptionsUniversal();
  return GET_CPLUSPLUS_POINTER(opt);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    setSizeRatio
 * Signature: (JI)V
 */
void Java_org_forstdb_CompactionOptionsUniversal_setSizeRatio(
    JNIEnv*, jobject, jlong jhandle, jint jsize_ratio) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->size_ratio = static_cast<unsigned int>(jsize_ratio);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    sizeRatio
 * Signature: (J)I
 */
jint Java_org_forstdb_CompactionOptionsUniversal_sizeRatio(JNIEnv*, jobject,
                                                           jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->size_ratio);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    setMinMergeWidth
 * Signature: (JI)V
 */
void Java_org_forstdb_CompactionOptionsUniversal_setMinMergeWidth(
    JNIEnv*, jobject, jlong jhandle, jint jmin_merge_width) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->min_merge_width = static_cast<unsigned int>(jmin_merge_width);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    minMergeWidth
 * Signature: (J)I
 */
jint Java_org_forstdb_CompactionOptionsUniversal_minMergeWidth(JNIEnv*, jobject,
                                                               jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->min_merge_width);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    setMaxMergeWidth
 * Signature: (JI)V
 */
void Java_org_forstdb_CompactionOptionsUniversal_setMaxMergeWidth(
    JNIEnv*, jobject, jlong jhandle, jint jmax_merge_width) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->max_merge_width = static_cast<unsigned int>(jmax_merge_width);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    maxMergeWidth
 * Signature: (J)I
 */
jint Java_org_forstdb_CompactionOptionsUniversal_maxMergeWidth(JNIEnv*, jobject,
                                                               jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->max_merge_width);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    setMaxSizeAmplificationPercent
 * Signature: (JI)V
 */
void Java_org_forstdb_CompactionOptionsUniversal_setMaxSizeAmplificationPercent(
    JNIEnv*, jobject, jlong jhandle, jint jmax_size_amplification_percent) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->max_size_amplification_percent =
      static_cast<unsigned int>(jmax_size_amplification_percent);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    maxSizeAmplificationPercent
 * Signature: (J)I
 */
jint Java_org_forstdb_CompactionOptionsUniversal_maxSizeAmplificationPercent(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->max_size_amplification_percent);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    setCompressionSizePercent
 * Signature: (JI)V
 */
void Java_org_forstdb_CompactionOptionsUniversal_setCompressionSizePercent(
    JNIEnv*, jobject, jlong jhandle, jint jcompression_size_percent) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->compression_size_percent =
      static_cast<unsigned int>(jcompression_size_percent);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    compressionSizePercent
 * Signature: (J)I
 */
jint Java_org_forstdb_CompactionOptionsUniversal_compressionSizePercent(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return static_cast<jint>(opt->compression_size_percent);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    setStopStyle
 * Signature: (JB)V
 */
void Java_org_forstdb_CompactionOptionsUniversal_setStopStyle(
    JNIEnv*, jobject, jlong jhandle, jbyte jstop_style_value) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->stop_style =
      ROCKSDB_NAMESPACE::CompactionStopStyleJni::toCppCompactionStopStyle(
          jstop_style_value);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    stopStyle
 * Signature: (J)B
 */
jbyte Java_org_forstdb_CompactionOptionsUniversal_stopStyle(JNIEnv*, jobject,
                                                            jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return ROCKSDB_NAMESPACE::CompactionStopStyleJni::toJavaCompactionStopStyle(
      opt->stop_style);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    setAllowTrivialMove
 * Signature: (JZ)V
 */
void Java_org_forstdb_CompactionOptionsUniversal_setAllowTrivialMove(
    JNIEnv*, jobject, jlong jhandle, jboolean jallow_trivial_move) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  opt->allow_trivial_move = static_cast<bool>(jallow_trivial_move);
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    allowTrivialMove
 * Signature: (J)Z
 */
jboolean Java_org_forstdb_CompactionOptionsUniversal_allowTrivialMove(
    JNIEnv*, jobject, jlong jhandle) {
  auto* opt =
      reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(jhandle);
  return opt->allow_trivial_move;
}

/*
 * Class:     org_forstdb_CompactionOptionsUniversal
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_forstdb_CompactionOptionsUniversal_disposeInternal(
    JNIEnv*, jobject, jlong jhandle) {
  delete reinterpret_cast<ROCKSDB_NAMESPACE::CompactionOptionsUniversal*>(
      jhandle);
}
