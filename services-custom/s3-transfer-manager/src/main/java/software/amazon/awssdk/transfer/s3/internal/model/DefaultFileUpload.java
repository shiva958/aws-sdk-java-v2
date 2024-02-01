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

package software.amazon.awssdk.transfer.s3.internal.model;

import java.io.File;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.services.s3.internal.multipart.PauseObservable;
import software.amazon.awssdk.services.s3.internal.multipart.S3ResumeToken;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.ResumableFileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferProgress;
import software.amazon.awssdk.utils.Lazy;
import software.amazon.awssdk.utils.ToString;
import software.amazon.awssdk.utils.Validate;

@SdkInternalApi
public final class DefaultFileUpload implements FileUpload {
    private final Lazy<ResumableFileUpload> resumableFileUpload;
    private final CompletableFuture<CompletedFileUpload> completionFuture;
    private final TransferProgress progress;
    private final UploadFileRequest request;
    private final PauseObservable pauseObservable;

    public DefaultFileUpload(CompletableFuture<CompletedFileUpload> completionFuture,
                             TransferProgress progress,
                             PauseObservable pauseObservable,
                             UploadFileRequest request) {
        this.completionFuture = Validate.paramNotNull(completionFuture, "completionFuture");
        this.progress = Validate.paramNotNull(progress, "progress");
        this.pauseObservable = Validate.paramNotNull(pauseObservable, "pauseObservable");
        this.request = Validate.paramNotNull(request, "request");
        this.resumableFileUpload = new Lazy<>(this::doPause);
    }

    @Override
    public ResumableFileUpload pause() {
        return resumableFileUpload.getValue();
    }

    private ResumableFileUpload doPause() {
        File sourceFile = request.source().toFile();
        Instant fileLastModified = Instant.ofEpochMilli(sourceFile.lastModified());

        if (completionFuture.isDone()) {
            System.out.println("Paused but was Done already");
            return ResumableFileUpload.builder()
                                      .fileLastModified(fileLastModified)
                                      .fileLength(sourceFile.length())
                                      .uploadFileRequest(request)
                                      .build();
        }

        S3ResumeToken token = null;
        try {
            token = pauseObservable.pause();
        } catch (Exception e) {
            //
        }

        // Upload hasn't started yet, or it's a single object upload
        if (token == null) {
            return ResumableFileUpload.builder()
                                      .fileLastModified(fileLastModified)
                                      .fileLength(sourceFile.length())
                                      .uploadFileRequest(request)
                                      .build();
        }

        return ResumableFileUpload.builder()
                                  .s3ResumeToken(token)
                                  .fileLastModified(fileLastModified)
                                  .fileLength(sourceFile.length())
                                  .uploadFileRequest(request)
                                  .multipartUploadId(token.uploadId())
                                  .build();
    }

    @Override
    public CompletableFuture<CompletedFileUpload> completionFuture() {
        return completionFuture;
    }

    @Override
    public TransferProgress progress() {
        return progress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultFileUpload that = (DefaultFileUpload) o;

        if (!resumableFileUpload.equals(that.resumableFileUpload)) {
            return false;
        }
        if (!pauseObservable.equals(that.pauseObservable)) {
            return false;
        }
        if (!completionFuture.equals(that.completionFuture)) {
            return false;
        }
        if (!progress.equals(that.progress)) {
            return false;
        }
        return request.equals(that.request);
    }

    @Override
    public int hashCode() {
        int result = resumableFileUpload.hashCode();
        result = 31 * result + pauseObservable.hashCode();
        result = 31 * result + completionFuture.hashCode();
        result = 31 * result + progress.hashCode();
        result = 31 * result + request.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return ToString.builder("DefaultFileUpload")
                       .add("completionFuture", completionFuture)
                       .add("progress", progress)
                       .add("request", request)
                       .build();
    }
}
