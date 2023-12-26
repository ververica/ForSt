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

#include "jni_helper.h"

#include <stdio.h>
#include <string.h>

#include "jexception.h"
#include "JvmUtils.h"

/** The Native return types that methods could return */
#define JVOID         'V'
#define JOBJECT       'L'
#define JARRAYOBJECT  '['
#define JBOOLEAN      'Z'
#define JBYTE         'B'
#define JCHAR         'C'
#define JSHORT        'S'
#define JINT          'I'
#define JLONG         'J'
#define JFLOAT        'F'
#define JDOUBLE       'D'

/**
 * Length of buffer for retrieving created JVMs.  (We only ever create one.)
 */
#define VM_BUF_LENGTH 1

void destroyLocalReference(JNIEnv *env, jobject jObject)
{
  if (jObject)
    env->DeleteLocalRef(jObject);
}

static jthrowable validateMethodType(JNIEnv *env, MethType methType)
{
    if (methType != STATIC && methType != INSTANCE) {
        return newRuntimeError(env, "validateMethodType(methType=%d): "
            "illegal method type.\n", methType);
    }
    return NULL;
}

jthrowable newJavaStr(JNIEnv *env, const char *str, jstring *out)
{
    jstring jstr;

    if (!str) {
        /* Can't pass NULL to NewStringUTF: the result would be
         * implementation-defined. */
        *out = NULL;
        return NULL;
    }
    jstr = env->NewStringUTF(str);
    if (!jstr) {
        /* If NewStringUTF returns NULL, an exception has been thrown,
         * which we need to handle.  Probaly an OOM. */
        return getPendingExceptionAndClear(env);
    }
    *out = jstr;
    return NULL;
}

jthrowable newCStr(JNIEnv *env, jstring jstr, char **out)
{
    const char *tmp;

    if (!jstr) {
        *out = NULL;
        return NULL;
    }
    tmp = env->GetStringUTFChars(jstr, NULL);
    if (!tmp) {
        return getPendingExceptionAndClear(env);
    }
    *out = strdup(tmp);
    env->ReleaseStringUTFChars(jstr, tmp);
    return NULL;
}

/**
 * Does the work to actually execute a Java method. Takes in an existing jclass
 * object and a va_list of arguments for the Java method to be invoked.
 */
static jthrowable invokeMethodOnJclass(JNIEnv *env, jvalue *retval,
        MethType methType, jobject instObj, jclass cls, const char *className,
        const char *methName, const char *methSignature, va_list args)
{
    jmethodID mid;
    jthrowable jthr;
    const char *str;
    char returnType;

    jthr = methodIdFromClass(cls, className, methName, methSignature, methType,
                             env, &mid);
    if (jthr)
        return jthr;
    str = methSignature;
    while (*str != ')') str++;
    str++;
    returnType = *str;
    if (returnType == JOBJECT || returnType == JARRAYOBJECT) {
        jobject jobj = NULL;
        if (methType == STATIC) {
            jobj = env->CallStaticObjectMethodV(cls, mid, args);
        }
        else if (methType == INSTANCE) {
            jobj = env->CallObjectMethodV(instObj, mid, args);
        }
        retval->l = jobj;
    }
    else if (returnType == JVOID) {
        if (methType == STATIC) {
            env->CallStaticVoidMethodV(cls, mid, args);
        }
        else if (methType == INSTANCE) {
            env->CallVoidMethodV(instObj, mid, args);
        }
    }
    else if (returnType == JBOOLEAN) {
        jboolean jbool = 0;
        if (methType == STATIC) {
            jbool = env->CallStaticBooleanMethodV(cls, mid, args);
        }
        else if (methType == INSTANCE) {
            jbool = env->CallBooleanMethodV(instObj, mid, args);
        }
        retval->z = jbool;
    }
    else if (returnType == JSHORT) {
        jshort js = 0;
        if (methType == STATIC) {
            js = env->CallStaticShortMethodV(cls, mid, args);
        }
        else if (methType == INSTANCE) {
            js = env->CallShortMethodV(instObj, mid, args);
        }
        retval->s = js;
    }
    else if (returnType == JLONG) {
        jlong jl = -1;
        if (methType == STATIC) {
            jl = env->CallStaticLongMethodV(cls, mid, args);
        }
        else if (methType == INSTANCE) {
            jl = env->CallLongMethodV(instObj, mid, args);
        }
        retval->j = jl;
    }
    else if (returnType == JINT) {
        jint ji = -1;
        if (methType == STATIC) {
            ji = env->CallStaticIntMethodV(cls, mid, args);
        }
        else if (methType == INSTANCE) {
            ji = env->CallIntMethodV(instObj, mid, args);
        }
        retval->i = ji;
    }

    jthr = env->ExceptionOccurred();
    if (jthr) {
        env->ExceptionClear();
        return jthr;
    }
    return NULL;
}

jthrowable findClassAndInvokeMethod(JNIEnv *env, jvalue *retval,
        MethType methType, jobject instObj, const char *className,
        const char *methName, const char *methSignature, ...)
{
    jclass cls = NULL;
    jthrowable jthr = NULL;

    va_list args;
    va_start(args, methSignature);

    jthr = validateMethodType(env, methType);
    if (jthr) {
        goto done;
    }

    cls = env->FindClass(className);
    if (!cls) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }

    jthr = invokeMethodOnJclass(env, retval, methType, instObj, cls,
            className, methName, methSignature, args);

done:
    va_end(args);
    destroyLocalReference(env, cls);
    return jthr;
}

jthrowable invokeMethod(JNIEnv *env, jvalue *retval, MethType methType,
        jobject instObj, CachedJavaClass cachedJavaClass,
        const char *methName, const char *methSignature, ...)
{
    jthrowable jthr;

    va_list args;
    va_start(args, methSignature);

    jthr = invokeMethodOnJclass(env, retval, methType, instObj,
            getJclass(cachedJavaClass),getClassName(cachedJavaClass),
                                methName, methSignature, args);

    va_end(args);
    return jthr;
}

static jthrowable constructNewObjectOfJclass(JNIEnv *env,
        jobject *out, jclass cls, const char *className,
                const char *ctorSignature, va_list args) {
    jmethodID mid;
    jobject jobj;
    jthrowable jthr;

    jthr = methodIdFromClass(cls, className, "<init>", ctorSignature, INSTANCE,
            env, &mid);
    if (jthr)
        return jthr;
    jobj = env->NewObjectV(cls, mid, args);
    if (!jobj)
        return getPendingExceptionAndClear(env);
    *out = jobj;
    return NULL;
}

jthrowable constructNewObjectOfClass(JNIEnv *env, jobject *out,
        const char *className, const char *ctorSignature, ...)
{
    va_list args;
    jclass cls;
    jthrowable jthr = NULL;

    cls = env->FindClass(className);
    if (!cls) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }

    va_start(args, ctorSignature);
    jthr = constructNewObjectOfJclass(env, out, cls, className,
            ctorSignature, args);
    va_end(args);
done:
    destroyLocalReference(env, cls);
    return jthr;
}

jthrowable constructNewObjectOfCachedClass(JNIEnv *env, jobject *out,
        CachedJavaClass cachedJavaClass, const char *ctorSignature, ...)
{
    jthrowable jthr = NULL;
    va_list args;
    va_start(args, ctorSignature);

    jthr = constructNewObjectOfJclass(env, out,
            getJclass(cachedJavaClass), getClassName(cachedJavaClass),
            ctorSignature, args);

    va_end(args);
    return jthr;
}

jthrowable methodIdFromClass(jclass cls, const char *className,
        const char *methName, const char *methSignature, MethType methType,
        JNIEnv *env, jmethodID *out)
{
    jthrowable jthr;
    jmethodID mid = 0;

    jthr = validateMethodType(env, methType);
    if (jthr)
        return jthr;
    if (methType == STATIC) {
        mid = env->GetStaticMethodID(cls, methName, methSignature);
    }
    else if (methType == INSTANCE) {
        mid = env->GetMethodID(cls, methName, methSignature);
    }
    if (mid == NULL) {
        fprintf(stderr, "could not find method %s from class %s with "
            "signature %s\n", methName, className, methSignature);
        return getPendingExceptionAndClear(env);
    }
    *out = mid;
    return NULL;
}

jthrowable classNameOfObject(jobject jobj, JNIEnv *env, char **name)
{
    jthrowable jthr;
    jclass cls, clsClass = NULL;
    jmethodID mid;
    jstring str = NULL;
    const char *cstr = NULL;
    char *newstr;

    cls = env->GetObjectClass(jobj);
    if (cls == NULL) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    clsClass = env->FindClass("java/lang/Class");
    if (clsClass == NULL) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    mid = env->GetMethodID(clsClass, "getName", "()Ljava/lang/String;");
    if (mid == NULL) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    str = static_cast<jstring>(env->CallObjectMethod(cls, mid));
    jthr = env->ExceptionOccurred();
    if (jthr) {
        env->ExceptionClear();
        goto done;
    }
    if (str == NULL) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    cstr = env->GetStringUTFChars(str, NULL);
    if (!cstr) {
        jthr = getPendingExceptionAndClear(env);
        goto done;
    }
    newstr = strdup(cstr);
    if (newstr == NULL) {
        jthr = newRuntimeError(env, "classNameOfObject: out of memory");
        goto done;
    }
    *name = newstr;
    jthr = NULL;

done:
    destroyLocalReference(env, cls);
    destroyLocalReference(env, clsClass);
    if (str) {
        if (cstr)
            env->ReleaseStringUTFChars(str, cstr);
        env->DeleteLocalRef(str);
    }
    return jthr;
}

int javaObjectIsOfClass(JNIEnv *env, jobject obj, const char *name)
{
    jclass clazz;
    int ret;

    clazz = env->FindClass(name);
    if (!clazz) {
        printPendingExceptionAndFree(env, PRINT_EXC_ALL,
            "javaObjectIsOfClass(%s)", name);
        return -1;
    }
    ret = env->IsInstanceOf(obj, clazz);
    env->DeleteLocalRef(clazz);
    return ret == JNI_TRUE ? 1 : 0;
}

