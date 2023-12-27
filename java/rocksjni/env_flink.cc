// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file implements the "bridge" between Java and C++ and enables
// calling c++ ROCKSDB_NAMESPACE::Env methods from Java side.

#include <jni.h>
#include <vector>

#include "java/rocksjni/portal.h"
#include "env/flink/env_flink.h"
#include "rocksdb/env.h"
#include "java/include/org_rocksdb_Env.h"
#include "java/include/org_rocksdb_FlinkEnv.h"
#include "java/include/org_rocksdb_RocksEnv.h"
#include "java/include/org_rocksdb_RocksMemEnv.h"
#include "java/include/org_rocksdb_TimedEnv.h"
#include "env/flink/JvmUtils.h"

/*
 * Class:     org_rocksdb_FlinkEnv
 * Method:    createFlinkEnv
 * Signature: (Ljava/lang/String;)J
 */
jlong Java_org_rocksdb_FlinkEnv_createFlinkEnv(
    JNIEnv *env, jclass, jstring jfsname)
{
    jboolean has_exception = JNI_FALSE;
    auto fsname =
        ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jfsname, &has_exception);
    if (has_exception == JNI_TRUE)
    {
        // exception occurred
        return 0;
    }

    std::unique_ptr<ROCKSDB_NAMESPACE::Env> flink_env;
    auto status = ROCKSDB_NAMESPACE::NewFlinkEnv(fsname, &flink_env);
    if (!status.ok())
    {
        // error occurred
      ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(env, status);
      return 0;
    }
    auto ptr_as_handle = flink_env.release();
    return reinterpret_cast<jlong>(ptr_as_handle);
}

/*
 * Class:     org_rocksdb_FlinkEnv
 * Method:    testFileExits
 * Signature: (JLjava/lang/String;)Z
 */
jboolean Java_org_rocksdb_FlinkEnv_testFileExits
    (JNIEnv *jniEnv, jclass, jlong jhandle, jstring jpath) {
    auto *env = reinterpret_cast<ROCKSDB_NAMESPACE::Env *>(jhandle);
    jboolean has_exception = JNI_FALSE;
    auto path =
        ROCKSDB_NAMESPACE::JniUtil::copyStdString(jniEnv, jpath, &has_exception);
    if (has_exception == JNI_TRUE) {
      return false;
    }
    auto status = env->FileExists(path);
    if (status.ok()) {
      return JNI_TRUE;
    } else if (status.IsNotFound()) {
      return JNI_FALSE;
    }
    ROCKSDB_NAMESPACE::RocksDBExceptionJni::ThrowNew(jniEnv, status);
    return JNI_FALSE;
}

/*
 * Class:     org_rocksdb_FlinkEnv
 * Method:    testLoadClass
 * Signature: (JLjava/lang/String;)J
 */
void Java_org_rocksdb_FlinkEnv_testLoadClass
    (JNIEnv * env, jclass, jlong, jstring jclassName) {
    jboolean has_exception = JNI_FALSE;
    auto className =
        ROCKSDB_NAMESPACE::JniUtil::copyStdString(env, jclassName, &has_exception);
    if (has_exception == JNI_TRUE) {
      // exception occurred
      return;
    }
    JNIEnv* jniEnv = FLINK_NAMESPACE::getJNIEnv(true);
    jclass testClass = jniEnv->FindClass(className.c_str());
    if (testClass == nullptr) {
      std::cerr << "Class " << className << " not found!" << std::endl;
      return;
    }

    jmethodID constructor = jniEnv->GetMethodID(testClass, "<init>", "()V");
    if (constructor == nullptr) {
      std::cerr << "No default constructor found for class " << className << std::endl;
      return;
    }

    jobject obj = jniEnv->NewObject(testClass, constructor);
    if (obj == nullptr) {
      std::cerr << "Could not create instance of class " << className << std::endl;
      return;
    }

    jmethodID midToString = jniEnv->GetMethodID(testClass, "toString", "()Ljava/lang/String;");
    if (midToString == nullptr) {
      std::cerr << "Method toString() not found!" << std::endl;
      return;
    }

    jstring result = static_cast<jstring>(jniEnv->CallObjectMethod(obj, midToString));

    const char* str = jniEnv->GetStringUTFChars(result, nullptr);
    if (str == nullptr) {
      std::cerr << "Out of memory." << std::endl;
      return;
    }

    std::cout << str << std::endl;
    env->ReleaseStringUTFChars(result, str);

    env->DeleteLocalRef(result);
    env->DeleteLocalRef(obj);
    env->DeleteLocalRef(testClass);
}

/*
 * Class:     org_rocksdb_FlinkEnv
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_FlinkEnv_disposeInternal(
    JNIEnv *, jobject, jlong jhandle)
{
    auto *e = reinterpret_cast<ROCKSDB_NAMESPACE::Env *>(jhandle);
    assert(e != nullptr);
    delete e;
}