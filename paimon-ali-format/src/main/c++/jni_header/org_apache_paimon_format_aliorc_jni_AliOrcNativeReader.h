/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_apache_paimon_format_aliorc_jni_AliOrcNativeReader */

#ifndef _Included_org_apache_paimon_format_aliorc_jni_AliOrcNativeReader
#define _Included_org_apache_paimon_format_aliorc_jni_AliOrcNativeReader
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_apache_paimon_format_aliorc_jni_AliOrcNativeReader
 * Method:    create
 * Signature: (Lorg/apache/paimon/fs/SeekableInputStream;JLjava/lang/String;I)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_format_aliorc_jni_AliOrcNativeReader_create
  (JNIEnv *, jobject, jobject, jlong, jstring, jint);

/*
 * Class:     org_apache_paimon_format_aliorc_jni_AliOrcNativeReader
 * Method:    readBatch
 * Signature: (JJ)V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_format_aliorc_jni_AliOrcNativeReader_readBatch
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     org_apache_paimon_format_aliorc_jni_AliOrcNativeReader
 * Method:    close
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_apache_paimon_format_aliorc_jni_AliOrcNativeReader_close
  (JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif
