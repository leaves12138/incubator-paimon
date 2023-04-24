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

package org.apache.paimon.operation;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.SimpleManifestEntry;
import org.apache.paimon.partition.PartitionTimeExtractor;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Expire partitions. */
public class PartitionExpire {

    private final List<String> partitionKeys;
    private final RowDataToObjectArrayConverter toObjectArrayConverter;
    private final Duration expirationTime;
    private final Duration checkInterval;
    private final PartitionTimeExtractor timeExtractor;
    private final FileStoreScan scan;
    private final FileStoreCommit commit;

    private LocalDateTime lastCheck;

    public PartitionExpire(
            RowType partitionType,
            Duration expirationTime,
            Duration checkInterval,
            String timePattern,
            String timeFormatter,
            FileStoreScan scan,
            FileStoreCommit commit) {
        this.partitionKeys = partitionType.getFieldNames();
        this.toObjectArrayConverter = new RowDataToObjectArrayConverter(partitionType);
        this.expirationTime = expirationTime;
        this.checkInterval = checkInterval;
        this.timeExtractor = new PartitionTimeExtractor(timePattern, timeFormatter);
        this.scan = scan;
        this.commit = commit;
        this.lastCheck = LocalDateTime.now();
    }

    public PartitionExpire withLock(Lock lock) {
        this.commit.withLock(lock);
        return this;
    }

    public void expire(long commitIdentifier) {
        expire(LocalDateTime.now(), commitIdentifier);
    }

    @VisibleForTesting
    void setLastCheck(LocalDateTime time) {
        lastCheck = time;
    }

    @VisibleForTesting
    void expire(LocalDateTime now, long commitIdentifier) {
        if (now.isAfter(lastCheck.plus(checkInterval))) {
            doExpire(now.minus(expirationTime), commitIdentifier);
            lastCheck = now;
        }
    }

    private void doExpire(LocalDateTime expireDateTime, long commitIdentifier) {
        List<BinaryRow> partitions = readPartitions();
        List<Map<String, String>> expired = new ArrayList<>();
        for (BinaryRow partition : partitions) {
            Object[] array = toObjectArrayConverter.convert(partition);
            LocalDateTime partTime = timeExtractor.extract(partitionKeys, Arrays.asList(array));
            if (expireDateTime.isAfter(partTime)) {
                expired.add(toPartitionString(array));
            }
        }

        if (expired.size() > 0) {
            commit.dropPartitions(expired, commitIdentifier);
        }
    }

    private Map<String, String> toPartitionString(Object[] array) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < partitionKeys.size(); i++) {
            map.put(partitionKeys.get(i), array[i].toString());
        }
        return map;
    }

    private List<BinaryRow> readPartitions() {
        return scan.simplePlan().files().stream()
                .map(SimpleManifestEntry::partition)
                .collect(Collectors.toList());
    }
}
