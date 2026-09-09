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

package org.apache.paimon.oss;

import org.apache.paimon.fs.MultiPartUploadStore;
import org.apache.paimon.fs.MultiPartUploadTwoPhaseOutputStream;
import org.apache.paimon.fs.Path;

import com.aliyun.sdk.service.oss2.models.CompleteMultipartUploadResult;

import java.io.IOException;

/** OSS two-phase output; parts remain invisible until the committer completes the upload. */
public class OssTwoPhaseOutputStream
        extends MultiPartUploadTwoPhaseOutputStream<OSSPartETag, CompleteMultipartUploadResult> {
    private final boolean overwrite;

    OssTwoPhaseOutputStream(
            MultiPartUploadStore<OSSPartETag, CompleteMultipartUploadResult> store,
            org.apache.hadoop.fs.Path path,
            Path target,
            boolean overwrite)
            throws IOException {
        super(store, path, target);
        this.overwrite = overwrite;
    }

    @Override
    public Committer committer() {
        return new OSSMultiPartUploadCommitter(
                uploadId, uploadedParts, objectName, position, targetPath, overwrite);
    }
}
