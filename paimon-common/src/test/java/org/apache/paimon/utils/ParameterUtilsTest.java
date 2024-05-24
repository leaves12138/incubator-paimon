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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ParameterUtilsTest {

    @Test
    public void testTableOptionsParse() {
        String need = "a=b, ab='a,b', cd = 'c,d', ef=   'e, f', '中国人'=  'Chinese person'  ";

        Map<String, String> expected = new HashMap<>();
        expected.put("a", "b");
        expected.put("ab", "a,b");
        expected.put("cd", "c,d");
        expected.put("ef", "e, f");
        expected.put("中国人", "Chinese person");
        Map<String, String> o = ParameterUtils.parseCommaSeparatedKeyValues(need);
        Assertions.assertThat(o).isEqualTo(expected);
    }

    @Test
    public void testTableOptionsParse2() {
        String need = "a=b, ab='a,b', cd = 'c,d', ef=   'e, f',";
        Assertions.assertThatCode(() -> ParameterUtils.parseCommaSeparatedKeyValues(need))
                .hasMessage(
                        "Can't resolve table option expression: a=b, ab='a,b', cd = 'c,d', ef=   'e, f',");
    }

    @Test
    public void testTableOptionsParse3() {
        String need = "  a='b',c=d,ef='   ef',  h = 'k', g=  'g', m='j', wq  =   wq";
        Map<String, String> expected = new HashMap<>();
        expected.put("a", "b");
        expected.put("ef", "   ef");
        expected.put("c", "d");
        expected.put("g", "g");
        expected.put("h", "k");
        expected.put("wq", "wq");
        expected.put("m", "j");
        Map<String, String> o = ParameterUtils.parseCommaSeparatedKeyValues(need);
        Assertions.assertThat(o).isEqualTo(expected);
    }
}
