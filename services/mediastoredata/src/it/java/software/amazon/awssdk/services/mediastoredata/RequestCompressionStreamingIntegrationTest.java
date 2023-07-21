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

package software.amazon.awssdk.services.mediastoredata;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.RequestCompressionConfiguration;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkInternalExecutionAttribute;
import software.amazon.awssdk.core.internal.compression.Compressor;
import software.amazon.awssdk.core.internal.compression.GzipCompressor;
import software.amazon.awssdk.core.internal.interceptor.trait.RequestCompression;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.mediastoredata.model.DeleteObjectRequest;
import software.amazon.awssdk.services.mediastoredata.model.GetObjectRequest;
import software.amazon.awssdk.services.mediastoredata.model.GetObjectResponse;
import software.amazon.awssdk.services.mediastoredata.model.ObjectNotFoundException;
import software.amazon.awssdk.services.mediastoredata.model.PutObjectRequest;
import software.amazon.awssdk.testutils.Waiter;

/**
 * Integration test to verify Request Compression functionalities for streaming operations. Do not delete.
 */
public class RequestCompressionStreamingIntegrationTest extends MediaStoreDataIntegrationTestBase {
    private static final String UNCOMPRESSED_BODY =
        "RequestCompressionTest-RequestCompressionTest-RequestCompressionTest-RequestCompressionTest-RequestCompressionTest";
    private static String compressedBody;
    private static MediaStoreDataClient syncClient;
    private static MediaStoreDataAsyncClient asyncClient;
    private static PutObjectRequest putObjectRequest;
    private static DeleteObjectRequest deleteObjectRequest;
    private static GetObjectRequest getObjectRequest;

    @BeforeAll
    public static void setup() {
        RequestCompressionConfiguration compressionConfiguration =
            RequestCompressionConfiguration.builder()
                                           .minimumCompressionThresholdInBytes(1)
                                           .requestCompressionEnabled(true)
                                           .build();

        RequestCompression requestCompressionTrait = RequestCompression.builder()
                                                                       .encodings("gzip")
                                                                       .isStreaming(true)
                                                                       .build();

        syncClient = MediaStoreDataClient.builder()
                                         .endpointOverride(uri)
                                         .credentialsProvider(credentialsProvider)
                                         .httpClient(ApacheHttpClient.builder().build())
                                         .overrideConfiguration(o -> o.addExecutionInterceptor(new CaptureTransferEncodingHeaderInterceptor())
                                                                      .addExecutionInterceptor(new CaptureContentEncodingHeaderInterceptor())
                                                                      .putExecutionAttribute(SdkInternalExecutionAttribute.REQUEST_COMPRESSION,
                                                                                             requestCompressionTrait)
                                                                      .requestCompressionConfiguration(compressionConfiguration))
                                         .build();

        asyncClient = MediaStoreDataAsyncClient.builder()
                                               .endpointOverride(uri)
                                               .credentialsProvider(getCredentialsProvider())
                                               .httpClient(NettyNioAsyncHttpClient.create())
                                               .overrideConfiguration(o -> o.addExecutionInterceptor(new CaptureTransferEncodingHeaderInterceptor())
                                                                            .addExecutionInterceptor(new CaptureContentEncodingHeaderInterceptor())
                                                                            .putExecutionAttribute(SdkInternalExecutionAttribute.REQUEST_COMPRESSION,
                                                                                                   requestCompressionTrait)
                                                                            .requestCompressionConfiguration(compressionConfiguration))
                                               .build();

        putObjectRequest = PutObjectRequest.builder()
                                           .contentType("application/octet-stream")
                                           .path("/foo")
                                           .overrideConfiguration(
                                               o -> o.requestCompressionConfiguration(
                                                   c -> c.requestCompressionEnabled(true)))
                                           .build();
        deleteObjectRequest = DeleteObjectRequest.builder().path("/foo").build();
        getObjectRequest = GetObjectRequest.builder().path("/foo").build();

        Compressor compressor = new GzipCompressor();
        byte[] compressedBodyBytes = compressor.compress(SdkBytes.fromUtf8String(UNCOMPRESSED_BODY)).asByteArray();
        compressedBody = new String(compressedBodyBytes);
    }

    @AfterAll
    public static void tearDown() {
        syncClient.deleteObject(deleteObjectRequest);
        Waiter.run(() -> syncClient.describeObject(r -> r.path("/foo")))
              .untilException(ObjectNotFoundException.class)
              .orFailAfter(Duration.ofMinutes(1));
    }

    @AfterEach
    public void cleanUp() {
        CaptureContentEncodingHeaderInterceptor.reset();
    }

    @Test
    public void putObject_withRequestCompressionSyncStreaming_compressesPayloadAndSendsCorrectly() throws IOException {
        TestContentProvider provider = new TestContentProvider(UNCOMPRESSED_BODY.getBytes(StandardCharsets.UTF_8));
        syncClient.putObject(putObjectRequest, RequestBody.fromContentProvider(provider, "binary/octet-stream"));

        assertThat(CaptureTransferEncodingHeaderInterceptor.isChunked).isTrue();
        assertThat(CaptureContentEncodingHeaderInterceptor.isGzip).isTrue();

        ResponseInputStream<GetObjectResponse> response = syncClient.getObject(getObjectRequest);
        byte[] buffer = new byte[UNCOMPRESSED_BODY.getBytes(StandardCharsets.UTF_8).length];
        response.read(buffer);
        String retrievedContent = new String(buffer);
        assertThat(UNCOMPRESSED_BODY).isEqualTo(retrievedContent);
    }

    // TODO : uncomment once async streaming compression is implemented
    /*@Test
    public void nettyClientPutObject_withoutContentLength_sendsSuccessfully() throws IOException {
        AsyncRequestBody asyncRequestBody = customAsyncRequestBodyWithoutContentLength();
        asyncClient.putObject(putObjectRequest, asyncRequestBody).join();

        assertThat(CaptureTransferEncodingHeaderInterceptor.isChunked).isTrue();
        assertThat(CaptureContentEncodingHeaderInterceptor.isGzip).isTrue();

        // verify stored content is correct
        ResponseInputStream<GetObjectResponse> response = syncClient.getObject(getObjectRequest);
        byte[] buffer = new byte[UNCOMPRESSED_BODY.getBytes(StandardCharsets.UTF_8).length];
        response.read(buffer);
        String retrievedContent = new String(buffer);
        assertThat(UNCOMPRESSED_BODY).isEqualTo(retrievedContent);
        assertThat(CaptureTransferEncodingHeaderInterceptor.isChunked).isTrue();
    }*/

    private static class CaptureContentEncodingHeaderInterceptor implements ExecutionInterceptor {
        public static boolean isGzip;

        public static void reset() {
            isGzip = false;
        }

        @Override
        public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            isGzip = context.httpRequest().matchingHeaders("Content-Encoding").contains("gzip");
        }
    }
}
