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

import org.apache.paimon.filter.FilterInterface;

import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** Bloom filter for secondary index. */
public class BloomFilter implements FilterInterface {

    public static final String BLOOM_FILTER = "bloom";

    private static final int HASH_NUMBER = 4;

    private final HadoopDynamicBloomFilter filter =
            new HadoopDynamicBloomFilter(50 * 8, HASH_NUMBER, Hash.MURMUR_HASH, 30);
    // reuse
    private final Key filterKey = new Key();

    public BloomFilter() {}

    public String name() {
        return BLOOM_FILTER;
    }

    @Override
    public void add(byte[] key) {
        filterKey.set(key, 1.0);
        filter.add(filterKey);
    }

    @Override
    public boolean testContains(byte[] key) {
        filterKey.set(key, 1.0);
        return filter.membershipTest(filterKey);
    }

    @Override
    public byte[] serializedBytes() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            filter.write(dos);
            byte[] bytes = baos.toByteArray();
            dos.close();
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BloomFilter recoverFrom(byte[] bytes) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));

        try {
            filter.readFields(dis);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return this;
    }
}