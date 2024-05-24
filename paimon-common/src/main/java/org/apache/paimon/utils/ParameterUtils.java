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

package org.apache.paimon.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** This is a util class for converting string parameter to another format. */
public class ParameterUtils {

    public static List<Map<String, String>> getPartitions(String... partitionStrings) {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (String partition : partitionStrings) {
            partitions.add(parseCommaSeparatedKeyValues(partition));
        }
        return partitions;
    }

    public static Map<String, String> parseCommaSeparatedKeyValues(String keyValues) {

        // use state machine to parse table options
        final int state1 = 1;
        final int state2 = 2;
        final int state3 = 3;
        final int state4 = 4;
        final int state5 = 5;
        final int state6 = 6;
        final int state7 = 7;
        final int state8 = 8;

        Map<String, String> kvs = new HashMap<>();
        StringBuilder key = new StringBuilder();
        StringBuilder value = new StringBuilder();
        if (!StringUtils.isBlank(keyValues)) {
            // init state
            int currentSearchState = state1;
            char[] chars = keyValues.toCharArray();
            int position = 0;
            Character next = chars[position];
            while (true) {
                if (next == null && currentSearchState != state7 && currentSearchState != state8) {
                    throw new IllegalArgumentException(
                            "Can't resolve table option expression: " + keyValues);
                }
                switch (currentSearchState) {
                    case state1:
                        {
                            if (next == '\'') {
                                currentSearchState = state2;
                            } else if (next == ' ') {

                            } else {
                                key.append(next);
                                currentSearchState = state4;
                            }
                            break;
                        }

                    case state2:
                        {
                            if (next == '\'') {
                                currentSearchState = state3;
                            } else {
                                key.append(next);
                            }
                            break;
                        }

                    case state3:
                        {
                            if (next == '=') {
                                currentSearchState = state5;
                            } else if (next == ' ') {
                                // no nothing
                            } else {
                                throw new IllegalArgumentException(
                                        "Error happens in position " + position + ", '=' expected");
                            }
                            break;
                        }

                    case state4:
                        {
                            if (next == '=') {
                                currentSearchState = state5;
                            } else if (next == ' ') {

                            } else {
                                key.append(next);
                            }
                            break;
                        }

                    case state5:
                        {
                            if (next == ' ') {

                            } else if (next == '\'') {
                                currentSearchState = state6;
                            } else {
                                value.append(next);
                                currentSearchState = state8;
                            }
                            break;
                        }

                    case state6:
                        {
                            if (next == '\'') {
                                currentSearchState = state7;
                            } else {
                                value.append(next);
                            }
                            break;
                        }

                    case state7:
                        {
                            kvs.put(key.toString(), value.toString());
                            if (next == null) {
                                return kvs;
                            } else if (next == ',') {
                                key = new StringBuilder();
                                value = new StringBuilder();
                                currentSearchState = state1;
                            } else if (next == ' ') {
                                // no nothing
                            } else {
                                throw new IllegalArgumentException(
                                        "Error happens in position " + position + ", '=' expected");
                            }
                            break;
                        }

                    case state8:
                        {
                            if (next == null) {
                                kvs.put(key.toString(), value.toString());
                                return kvs;
                            } else if (next == ',') {
                                kvs.put(key.toString(), value.toString());
                                key = new StringBuilder();
                                value = new StringBuilder();
                                currentSearchState = state1;
                            } else if (next == ' ') {

                            } else {
                                value.append(next);
                            }
                            break;
                        }
                }

                position++;
                next = position < chars.length ? chars[position] : null;
            }
        }
        return kvs;
    }

    public static void parseKeyValueString(Map<String, String> map, String kvString) {
        String[] kv = kvString.split("=", 2);
        if (kv.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid key-value string '%s'. Please use format 'key=value'",
                            kvString));
        }
        map.put(kv[0].trim(), kv[1].trim());
    }
}
