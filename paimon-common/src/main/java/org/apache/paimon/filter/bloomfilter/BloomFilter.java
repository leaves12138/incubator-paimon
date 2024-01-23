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

package org.apache.paimon.filter.bloomfilter;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.paimon.filter.FilterInterface;

import org.apache.hadoop.util.bloom.Key;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class BloomFilter implements FilterInterface {

    org.apache.hadoop.util.bloom.BloomFilter filter =
            new org.apache.hadoop.util.bloom.BloomFilter();
    Key filterKey = new Key();

    public BloomFilter() {}

    public BloomFilter(String serString) {
        this.filter = new org.apache.hadoop.util.bloom.BloomFilter();
        byte[] bytes = Base64.getDecoder().decode(serString.getBytes(StandardCharsets.UTF_8));
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            this.filter.readFields(dis);
            dis.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void add(byte[] key) {
        filterKey.set(key, 1.0);
        filter.add(filterKey);
    }

    @Override
    public boolean test(byte[] key) {
        filterKey.set(key, 1.0);
        return filter.membershipTest(filterKey);
    }

    @Override
    public String serializeToString() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(filter.getVectorSize() / 8 + 1);
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            filter.write(dos);
            byte[] bytes = baos.toByteArray();
            dos.close();
            return new String(Base64.getEncoder().encode(bytes), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void fromSerializedString(String serString) {
        byte[] bytes = Base64.getDecoder().decode(serString.getBytes(StandardCharsets.UTF_8));
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));

        try {
            filter.readFields(dis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
