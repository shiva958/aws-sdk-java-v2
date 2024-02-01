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


import static software.amazon.awssdk.services.s3.internal.multipart.UploadObjectHelper.PAUSE_OBSERVABLE;
import static software.amazon.awssdk.services.s3.internal.multipart.UploadObjectHelper.RESUME_TOKEN;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.utils.Logger;
import software.amazon.awssdk.utils.Pair;

/**
 * An internal helper class that automatically uses multipart upload based on the size of the object.
 */
@SdkInternalApi
public final class UploadWithKnownContentLengthHelper {
    private static final Logger log = Logger.loggerFor(UploadWithKnownContentLengthHelper.class);

    private final S3AsyncClient s3AsyncClient;
    private final long partSizeInBytes;
    private final GenericMultipartHelper<PutObjectRequest, PutObjectResponse> genericMultipartHelper;

    private final long maxMemoryUsageInBytes;
    private final long multipartUploadThresholdInBytes;
    private final MultipartUploadHelper multipartUploadHelper;

    public UploadWithKnownContentLengthHelper(S3AsyncClient s3AsyncClient,
                                              long partSizeInBytes,
                                              long multipartUploadThresholdInBytes,
                                              long maxMemoryUsageInBytes) {
        this.s3AsyncClient = s3AsyncClient;
        this.partSizeInBytes = partSizeInBytes;
        this.genericMultipartHelper = new GenericMultipartHelper<>(s3AsyncClient,
                                                                   SdkPojoConversionUtils::toAbortMultipartUploadRequest,
                                                                   SdkPojoConversionUtils::toPutObjectResponse);
        this.maxMemoryUsageInBytes = maxMemoryUsageInBytes;
        this.multipartUploadThresholdInBytes = multipartUploadThresholdInBytes;
        this.multipartUploadHelper = new MultipartUploadHelper(s3AsyncClient, partSizeInBytes, multipartUploadThresholdInBytes,
                                                               maxMemoryUsageInBytes);
    }

    public CompletableFuture<PutObjectResponse> uploadObject(PutObjectRequest putObjectRequest,
                                                             AsyncRequestBody asyncRequestBody,
                                                             long contentLength) {
        CompletableFuture<PutObjectResponse> returnFuture = new CompletableFuture<>();

        // TODO - explore alternative -> add S3ResumeToken / upload ID directly to PauseObservable
        /*PauseObservable pauseObservable = putObjectRequest.overrideConfiguration().map(c -> c.executionAttributes().getAttribute(PAUSE_OBSERVABLE)).orElse(null);
        S3ResumeToken resumeToken = pauseObservable.s3ResumeToken();*/

        S3ResumeToken resumeToken = putObjectRequest.overrideConfiguration().map(c -> c.executionAttributes().getAttribute(RESUME_TOKEN)).orElse(null);
        try {
            if (contentLength > multipartUploadThresholdInBytes && contentLength > partSizeInBytes) {
                log.debug(() -> "Starting the upload as multipart upload request");
                uploadInParts(putObjectRequest, contentLength, asyncRequestBody, returnFuture, resumeToken);
            } else {
                log.debug(() -> "Starting the upload as a single upload part request");
                multipartUploadHelper.uploadInOneChunk(putObjectRequest, asyncRequestBody, returnFuture);
            }

        } catch (Throwable throwable) {
            returnFuture.completeExceptionally(throwable);
        }

        return returnFuture;
    }

    private void uploadInParts(PutObjectRequest putObjectRequest, long contentLength, AsyncRequestBody asyncRequestBody,
                               CompletableFuture<PutObjectResponse> returnFuture, S3ResumeToken s3ResumeToken) {
        String uploadId;
        HashMap<Integer, CompletedPart> existingParts = null;
        if (s3ResumeToken == null) {
            uploadId = multipartUploadHelper.createMultipartUpload(putObjectRequest, returnFuture).join().uploadId();
            log.debug(() -> "Initiated a new multipart upload, uploadId: " + uploadId);
        } else {
            uploadId = s3ResumeToken.uploadId();
            existingParts = identifyExistingPartsForResume(uploadId, putObjectRequest);
            log.debug(() -> "Resuming a paused multipart upload, uploadId: " + s3ResumeToken.uploadId());
        }

        doUploadInParts(Pair.of(putObjectRequest, asyncRequestBody), contentLength, returnFuture, uploadId,
                        existingParts);
    }

    private void doUploadInParts(Pair<PutObjectRequest, AsyncRequestBody> request,
                                 long contentLength,
                                 CompletableFuture<PutObjectResponse> returnFuture,
                                 String uploadId,
                                 HashMap<Integer, CompletedPart> existingParts) {

        long optimalPartSize = genericMultipartHelper.calculateOptimalPartSizeFor(contentLength, partSizeInBytes);
        int partCount = genericMultipartHelper.determinePartCount(contentLength, optimalPartSize);
        if (optimalPartSize > partSizeInBytes) {
            log.debug(() -> String.format("Configured partSize is %d, but using %d to prevent reaching maximum number of parts "
                                          + "allowed", partSizeInBytes, optimalPartSize));
        }

        log.debug(() -> String.format("Starting multipart upload with partCount: %d, optimalPartSize: %d", partCount,
                                      optimalPartSize));

        MpuRequestContext mpuRequestContext =
            new MpuRequestContext(request, contentLength, optimalPartSize, uploadId, existingParts);
        KnownContentLengthAsyncRequestBodySubscriber subscriber = new KnownContentLengthAsyncRequestBodySubscriber(mpuRequestContext, returnFuture);
        attachSubscriberToObservable(subscriber, request.left());


        request.right()
               .split(b -> b.chunkSizeInBytes(mpuRequestContext.partSize)
                            .bufferSizeInBytes(maxMemoryUsageInBytes))
               .subscribe(subscriber);
    }

    public HashMap<Integer, CompletedPart> identifyExistingPartsForResume(String uploadId, PutObjectRequest putObjectRequest) {
        HashMap<Integer, CompletedPart> existingParts= new HashMap<>();
        int partNumberMarker = 0;

        while (true) {
            ListPartsRequest request = ListPartsRequest.builder()
                                                       .uploadId(uploadId)
                                                       .bucket(putObjectRequest.bucket())
                                                       .key(putObjectRequest.key())
                                                       .partNumberMarker(partNumberMarker)
                                                       .build();

            ListPartsResponse response = s3AsyncClient.listParts(request).join();
            for (Part part : response.parts()) {
                existingParts.put(part.partNumber(), SdkPojoConversionUtils.toCompletedPart(part));
            }
            if (!response.isTruncated()) {
                return existingParts;
            }
            partNumberMarker = response.nextPartNumberMarker();
        }
    }

    private void attachSubscriberToObservable(KnownContentLengthAsyncRequestBodySubscriber subscriber,
                                              PutObjectRequest putObjectRequest) {
        PauseObservable pauseObservable = putObjectRequest.overrideConfiguration().get().executionAttributes().getAttribute(PAUSE_OBSERVABLE);
        pauseObservable.setSubscriber(subscriber);
    }

    private static final class MpuRequestContext {
        private final Pair<PutObjectRequest, AsyncRequestBody> request;
        private final long contentLength;
        private final long partSize;
        private final String uploadId;
        private final HashMap<Integer, CompletedPart> existingParts;

        private MpuRequestContext(Pair<PutObjectRequest, AsyncRequestBody> request,
                                  long contentLength,
                                  long partSize,
                                  String uploadId,
                                  HashMap<Integer, CompletedPart> existingParts) {
            this.request = request;
            this.contentLength = contentLength;
            this.partSize = partSize;
            this.uploadId = uploadId;
            this.existingParts = existingParts;
        }
    }

    // separate class?
    public class KnownContentLengthAsyncRequestBodySubscriber implements Subscriber<AsyncRequestBody> {

        /**
         * The number of AsyncRequestBody has been received but yet to be processed
         */
        private final AtomicInteger asyncRequestBodyInFlight = new AtomicInteger(0);

        /**
         * Indicates whether CompleteMultipart has been initiated or not.
         */
        private final AtomicBoolean completedMultipartInitiated = new AtomicBoolean(false);

        private final AtomicBoolean failureActionInitiated = new AtomicBoolean(false);

        private final AtomicInteger partNumber = new AtomicInteger(1);

        private final AtomicReferenceArray<CompletedPart> completedParts;
        private final String uploadId;
        private final Collection<CompletableFuture<CompletedPart>> futures = new ConcurrentLinkedQueue<>();

        private final PutObjectRequest putObjectRequest;
        private final CompletableFuture<PutObjectResponse> returnFuture;
        private Subscription subscription;
        private volatile boolean isDone;
        private volatile boolean isPaused;
        private final HashMap<Integer, CompletedPart> existingParts;
        private CompletableFuture<CompleteMultipartUploadResponse> completeFuture;

        KnownContentLengthAsyncRequestBodySubscriber(MpuRequestContext mpuRequestContext,
                                                     CompletableFuture<PutObjectResponse> returnFuture) {
            long optimalPartSize = genericMultipartHelper.calculateOptimalPartSizeFor(mpuRequestContext.contentLength,
                                                                                      partSizeInBytes);
            int partCount = genericMultipartHelper.determinePartCount(mpuRequestContext.contentLength, optimalPartSize);
            this.putObjectRequest = mpuRequestContext.request.left();
            this.returnFuture = returnFuture;
            this.completedParts = new AtomicReferenceArray<>(partCount);
            this.uploadId = mpuRequestContext.uploadId;
            this.existingParts = mpuRequestContext.existingParts;
        }

        public S3ResumeToken pause() {
            if (completeFuture != null && completeFuture.isDone()) {
                System.out.println("Tried PAUSE but was already DONE");
                return null;
            }

            if (completeFuture != null && !completeFuture.isDone()) {
                System.out.println("Cancelling CompletMultipartUpload future");
                completeFuture.cancel(true);
            }

            System.out.println("PAUSING");
            isPaused = true;

            // Cancel all in progress uploads
            for (CompletableFuture<CompletedPart> cf : futures) {
                if (!cf.isDone()) {
                    System.out.println("Cancelling UploadPart future");
                    cf.cancel(true);
                }
            }

            return new S3ResumeToken(uploadId);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (this.subscription != null) {
                log.warn(() -> "The subscriber has already been subscribed. Cancelling the incoming subscription");
                subscription.cancel();
                return;
            }
            this.subscription = s;
            s.request(1);
            returnFuture.whenComplete((r, t) -> {
                if (t != null) {
                    s.cancel();
                    if (failureActionInitiated.compareAndSet(false, true) && !isPaused){
                        multipartUploadHelper.failRequestsElegantly(futures, t, uploadId, returnFuture, putObjectRequest);
                    }
                }
            });
        }

        @Override
        public void onNext(AsyncRequestBody asyncRequestBody) {
            if (isPaused) {
                System.out.println("MPU is paused, will not send part #" + partNumber.get());
                return;
            }

            int currentPartNum = partNumber.getAndIncrement();

            // Resume - skip
            if (existingParts != null && existingParts.containsKey(currentPartNum)) {
                System.out.println("Skipping already uploaded part - #" + currentPartNum);
                subscription.request(1);
                // TODO - signal completion
                return;
            }

            System.out.println("KnownLengthSubscriber - onNext() - uploading part #" + currentPartNum);
            asyncRequestBodyInFlight.incrementAndGet();
            UploadPartRequest uploadRequest = SdkPojoConversionUtils.toUploadPartRequest(putObjectRequest, currentPartNum, uploadId);

            Consumer<CompletedPart> completedPartConsumer = completedPart -> completedParts.set(completedPart.partNumber() - 1,
                                                                                                completedPart);
            multipartUploadHelper.sendIndividualUploadPartRequest(uploadId, completedPartConsumer, futures, Pair.of(uploadRequest, asyncRequestBody))
                                 .whenComplete((r, t) -> {
                                     if (t != null) {
                                         if (failureActionInitiated.compareAndSet(false, true) && !isPaused) {
                                             multipartUploadHelper.failRequestsElegantly(futures, t, uploadId, returnFuture, putObjectRequest);
                                         }
                                     } else {
                                         System.out.println("Finished sending part #" + currentPartNum);
                                         completeMultipartUploadIfFinish(asyncRequestBodyInFlight.decrementAndGet());
                                     }
                                 });
            subscription.request(1);
        }

        @Override
        public void onError(Throwable t) {
            log.debug(() -> "Received onError ", t);
            if (failureActionInitiated.compareAndSet(false, true)) {
                multipartUploadHelper.failRequestsElegantly(futures, t, uploadId, returnFuture, putObjectRequest);
            }
        }

        @Override
        public void onComplete() {
            log.debug(() -> "Received onComplete()");
            isDone = true;
            System.out.println("Checkpoint - onComplete() -> Completing MPU ====> isPaused - " + isPaused);
            if (!isPaused) {
                completeMultipartUploadIfFinish(asyncRequestBodyInFlight.get());
            }
        }

        private void completeMultipartUploadIfFinish(int requestsInFlight) {
            if (isDone && requestsInFlight == 0) {

                CompletedPart[] parts;

                if (existingParts != null) {
                    parts = Stream.concat(IntStream.range(0, completedParts.length() + existingParts.size())
                                                   .mapToObj(completedParts::get),
                                          existingParts.values().stream())
                                  .toArray(CompletedPart[]::new);
                } else {
                    parts = IntStream.range(0, completedParts.length())
                                     .mapToObj(completedParts::get)
                                     .toArray(CompletedPart[]::new);

                }
                completeFuture = multipartUploadHelper.completeMultipartUpload(returnFuture, uploadId, parts, putObjectRequest);
            }
            }
        }


}
