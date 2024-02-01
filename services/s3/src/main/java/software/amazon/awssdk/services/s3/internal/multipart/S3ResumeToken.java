/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.services.s3.internal.multipart;

import java.util.Objects;
import software.amazon.awssdk.annotations.SdkInternalApi;

@SdkInternalApi
public class S3ResumeToken {
    private final String uploadId;
    private long partSize;
    private long totalNumParts;
    private long numPartsCompleted;

    public S3ResumeToken(String uploadId) {
        this.uploadId = uploadId;
    }

    // To make resume() work with CRT S3Client, need to add these fields, though they are not needed when resuming with JavaS3Client
    /*public S3ResumeToken(String uploadId, long partSize, long totalNumParts, long numPartsCompleted) {
        this.uploadId = uploadId;
        this.partSize = partSize;
        this.totalNumParts = totalNumParts;
        this.numPartsCompleted = numPartsCompleted;
    }*/

    // equals() + hashCode()

    public String uploadId() {
        return uploadId;
    }

    public long partSize() {
        return partSize;
    }

    public long totalNumParts() {
        return totalNumParts;
    }

    public long numPartsCompleted() {
        return numPartsCompleted;
    }
}
