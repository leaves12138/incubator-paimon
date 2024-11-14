/*
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

package org.apache.paimon.format.common.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;

/** Utils for jni. */
public class JNIUtils {

    private static boolean inited = false;
    private static final String jniName = "paimon_jni";

    private static String osName() {
        String osName = System.getProperty("os.name").toLowerCase().replace(' ', '_');
        if (osName.startsWith("win")) {
            return "win";
        } else {
            return osName.startsWith("mac") ? "darwin" : osName;
        }
    }

    private static String osArch() {
        return System.getProperty("os.arch");
    }

    private static String libExtension() {
        if (!osName().contains("os_x") && !osName().contains("darwin")) {
            return osName().contains("win") ? "dll" : "so";
        } else {
            return "dylib";
        }
    }

    private static String resourceName() {
        return "/" + osName() + "/" + osArch() + "/" + "libpaimon_jni" + "." + libExtension();
    }

    private static void loadLibraryFile(final String fileName) {
        AccessController.doPrivileged(
                (PrivilegedAction<Void>)
                        () -> {
                            System.load(fileName);
                            return null;
                        });
    }

    public static synchronized void load() {
        if (!inited) {
            String jniFileName = resourceName();
            InputStream jniFileInput = JNIUtils.class.getResourceAsStream(jniFileName);
            if (jniFileInput == null) {
                throw new UnsatisfiedLinkError(
                        "Can't find " + resourceName() + "  to link arrow file io");
            } else {
                File tempFile = null;
                FileOutputStream fin = null;

                try {
                    tempFile = File.createTempFile("lib" + jniName, "." + libExtension());
                    tempFile.deleteOnExit();
                    fin = new FileOutputStream(tempFile);
                    byte[] bytes = new byte[4096];

                    while (true) {
                        int bSize = jniFileInput.read(bytes);
                        if (bSize == -1) {
                            try {
                                fin.flush();
                                fin.close();
                                fin = null;
                            } catch (IOException ignored) {
                            }

                            loadLibraryFile(tempFile.getAbsolutePath());
                            inited = true;
                            return;
                        }

                        fin.write(bytes, 0, bSize);
                    }
                } catch (IOException ioException) {
                    ExceptionInInitializerError exceptionInInitializerError =
                            new ExceptionInInitializerError(
                                    "Cannot unpack lib"
                                            + jniName
                                            + ": "
                                            + ioException.getMessage());
                    exceptionInInitializerError.setStackTrace(ioException.getStackTrace());
                    throw exceptionInInitializerError;
                } finally {
                    try {
                        jniFileInput.close();
                        if (fin != null) {
                            fin.close();
                        }

                        if (tempFile != null && tempFile.exists()) {
                            tempFile.delete();
                        }
                    } catch (IOException ignored) {
                    }
                }
            }
        }
    }
}
