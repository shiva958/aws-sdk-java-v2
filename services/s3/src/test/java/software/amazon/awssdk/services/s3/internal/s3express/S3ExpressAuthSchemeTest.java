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

package software.amazon.awssdk.services.s3.internal.s3express;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.spi.scheme.AuthScheme;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.s3express.S3ExpressAuthScheme;
import software.amazon.awssdk.services.s3.s3express.S3ExpressHttpSigner;
import software.amazon.awssdk.services.s3.s3express.S3ExpressIdentityProvider;
import software.amazon.awssdk.services.s3.s3express.S3ExpressSessionCredentials;

public class S3ExpressAuthSchemeTest {

    @Test
    void identityProvider_noAwsCredentialProviders_throwsException() {
        DefaultS3ExpressAuthScheme defaultS3ExpressAuthScheme = DefaultS3ExpressAuthScheme.create();

        IdentityProviders eligibleIdentityProviders = IdentityProviders.builder().build();
        assertThatThrownBy(() -> defaultS3ExpressAuthScheme.identityProvider(eligibleIdentityProviders))
            .isInstanceOf(IllegalStateException.class).hasMessage("Could not find a provider for AwsCredentialsIdentity");
    }

    @Test
    void identityProvider_oneEligibleProvider_works() {
        DefaultS3ExpressAuthScheme defaultS3ExpressAuthScheme = DefaultS3ExpressAuthScheme.create();

        IdentityProviders eligibleIdentityProviders = IdentityProviders.builder()
                                                                       .putIdentityProvider(DefaultCredentialsProvider.create())
                                                                       .build();
        IdentityProvider<S3ExpressSessionCredentials> s3ExpressIdentityProvider =
            defaultS3ExpressAuthScheme.identityProvider(eligibleIdentityProviders);
        assertThat(s3ExpressIdentityProvider).isInstanceOf(DefaultS3ExpressIdentityProvider.class);
    }

    @Test
    void customS3ExpressAuthScheme() {
        List<String> headersToExclude = Arrays.asList("header1", "header2", "header3");
        ExcludeHeadersSigner signer = new ExcludeHeadersSigner(S3ExpressHttpSigner.create(), headersToExclude);
        S3ExpressAuthScheme x = new CustomS3ExpressAuthScheme(signer);

        S3Client client = S3Client.builder()
                                  .putAuthScheme(x)
                                  .build();

        //make any API call
        client.putObject(r -> r.bucket("bucket").key("key"), RequestBody.fromString("test"));
    }

    static class ExcludeHeadersSigner implements S3ExpressHttpSigner {
        private final S3ExpressHttpSigner delegateSigner;
        private List<String> excludedHeaders;

        ExcludeHeadersSigner(S3ExpressHttpSigner delegateSigner, List<String> excludedHeaders) {
            this.delegateSigner = delegateSigner;
            this.excludedHeaders = excludedHeaders;
        }

        @Override
        public SignedRequest sign(SignRequest<? extends S3ExpressSessionCredentials> request) {
            SdkHttpRequest unsignedHttpRequest = request.request();

            Map<String, List<String>> requestHeadersToExclude = new HashMap<>();
            SdkHttpRequest.Builder httpRequestWithRemovedHeaders = unsignedHttpRequest.toBuilder();
            unsignedHttpRequest.headers().forEach((key, value) -> {
                if (excludedHeaders.contains(key)) {
                    requestHeadersToExclude.put(key, value);
                    httpRequestWithRemovedHeaders.removeHeader(key);
                }
            });

            SignRequest<? extends AwsCredentialsIdentity> modifiedRequestToSign =
                request.copy(c -> c.request(httpRequestWithRemovedHeaders.build()));

            SignedRequest signedRequest = delegateSigner.sign(modifiedRequestToSign);

            SdkHttpRequest signedHttpRequest = signedRequest.request();
            SdkHttpRequest.Builder httprequest = signedHttpRequest.toBuilder();
            requestHeadersToExclude.forEach(httprequest::putHeader);

            return signedRequest.copy(signed -> signed.request(httprequest.build()));
        }

        @Override
        public CompletableFuture<AsyncSignedRequest> signAsync(AsyncSignRequest<? extends S3ExpressSessionCredentials> request) {
            // implement like above, returning null in this example
            return CompletableFuture.completedFuture(null);
        }
    }

    static class CustomS3ExpressAuthScheme implements S3ExpressAuthScheme {
        private final S3ExpressHttpSigner signer;
        private final S3ExpressAuthScheme defaultScheme;

        CustomS3ExpressAuthScheme(S3ExpressHttpSigner signer) {
            this.signer = signer;
            this.defaultScheme = S3ExpressAuthScheme.create();
        }

        @Override
        public IdentityProvider<S3ExpressSessionCredentials> identityProvider(IdentityProviders providers) {
            return defaultScheme.identityProvider(providers);
        }

        @Override
        public S3ExpressHttpSigner signer() {
            return signer;
        }

        @Override
        public String schemeId() {
            return defaultScheme.schemeId();
        }
    }

}
