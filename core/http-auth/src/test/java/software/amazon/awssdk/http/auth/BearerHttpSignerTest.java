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

package software.amazon.awssdk.http.auth;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.spi.AsyncHttpSignRequest;
import software.amazon.awssdk.http.auth.spi.AsyncSignedHttpRequest;
import software.amazon.awssdk.http.auth.spi.SyncHttpSignRequest;
import software.amazon.awssdk.http.auth.spi.SyncSignedHttpRequest;
import software.amazon.awssdk.identity.spi.TokenIdentity;
import java.io.ByteArrayInputStream;
import java.net.URI;
import software.amazon.awssdk.utils.async.SimplePublisher;

import static org.assertj.core.api.Assertions.assertThat;

class BearerHttpSignerTest {

    private static final String BEARER_AUTH_MARKER = "Bearer ";

    @Test
    public void whenTokenExists_requestIsSignedCorrectly() {
        String tokenValue = "mF_9.B5f-4.1JqM";

        BearerHttpSigner tokenSigner = BearerHttpSigner.create();
        SyncSignedHttpRequest signedRequest = tokenSigner.sign(generateBasicRequest(tokenValue));


        String expectedHeader = createExpectedHeader(tokenValue);
        assertThat(signedRequest.request().firstMatchingHeader(
                "Authorization")).hasValue(expectedHeader);
    }

    @Test
    public void whenTokenExists_asyncRequestIsSignedCorrectly() {
        String tokenValue = "mF_9.B5f-4.1JqM";

        BearerHttpSigner tokenSigner = BearerHttpSigner.create();

        AsyncSignedHttpRequest signedRequest =
                tokenSigner.signAsync(generateBasicAsyncRequest(tokenValue));


        String expectedHeader = createExpectedHeader(tokenValue);
        assertThat(signedRequest.request().firstMatchingHeader(
                "Authorization")).hasValue(expectedHeader);
    }

    private static String createExpectedHeader(String token) {
        return BEARER_AUTH_MARKER + token;
    }

    private static SyncHttpSignRequest<? extends TokenIdentity> generateBasicRequest(String token) {

        return SyncHttpSignRequest.builder(TokenIdentity.create(token))
                .request(SdkHttpRequest.builder()
                        .method(SdkHttpMethod.POST)
                        .putHeader("Host", "demo.us-east-1.amazonaws.com")
                        .putHeader("x-amz-archive-description", "test  test")
                        .encodedPath("/")
                        .uri(URI.create("http://demo.us-east-1.amazonaws.com"))
                        .build())
                .payload(() -> new ByteArrayInputStream("{\"TableName\": \"foo\"}".getBytes()))
                .build();
    }

    private static AsyncHttpSignRequest<? extends TokenIdentity> generateBasicAsyncRequest(String token) {

        return AsyncHttpSignRequest.builder(TokenIdentity.create(token))
                .request(SdkHttpRequest.builder()
                        .method(SdkHttpMethod.POST)
                        .putHeader("Host", "demo.us-east-1.amazonaws.com")
                        .putHeader("x-amz-archive-description", "test  test")
                        .encodedPath("/")
                        .uri(URI.create("http://demo.us-east-1.amazonaws.com"))
                        .build())
                .payload(new SimplePublisher<>())
                .build();
    }

}
