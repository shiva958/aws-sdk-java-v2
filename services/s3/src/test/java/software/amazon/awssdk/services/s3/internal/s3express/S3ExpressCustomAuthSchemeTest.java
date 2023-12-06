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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.identity.SdkIdentityProperty;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignRequest;
import software.amazon.awssdk.http.auth.spi.signer.AsyncSignedRequest;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignRequest;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.IdentityProvider;
import software.amazon.awssdk.identity.spi.IdentityProviders;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.s3express.S3ExpressAuthScheme;
import software.amazon.awssdk.services.s3.s3express.S3ExpressSessionCredentials;

public class S3ExpressCustomAuthSchemeTest {

    private S3ExpressAuthScheme customS3ExpressAuthScheme;

    @BeforeEach
    void initClasses() {
        S3ExpressAuthScheme defaultS3ExpressAuthScheme = S3ExpressAuthScheme.create();

        List<String> headersToExclude = Arrays.asList("header1", "header2", "header3");
        CustomSigner customSigner = new CustomSigner(defaultS3ExpressAuthScheme.signer(), headersToExclude);

        customS3ExpressAuthScheme = new CustomS3ExpressAuthScheme(customSigner);
    }

    @Test
    void customS3ExpressAuthScheme_addedToClient() {
        S3Client client = S3Client.builder()
                                  .putAuthScheme(customS3ExpressAuthScheme)
                                  .build();

        //make any API call
        client.putObject(r -> r.bucket("bucket").key("key"), RequestBody.fromString("test"));
    }

    @Test
    void customS3ExpressAuthScheme_addedToPlugin() {
        // plugins allow more customizations than adding individual builder options
        SdkPlugin customPlugin = config -> {
            S3ServiceClientConfiguration.Builder s3Config = (S3ServiceClientConfiguration.Builder) config;

            //Overrides the standard S3ExpressAuthScheme
            s3Config.putAuthScheme(customS3ExpressAuthScheme);

            //Optional. Overrides the S3AuthSchemeProvider. ONLY do this if you want more identity or signer properties
            //          AND you know what you are doing
            // S3AuthSchemeProvider customAuthSchemeProvider = (S3AuthSchemeProvider) authSchemeParams -> {
            //     List<AuthSchemeOption> authSchemeOptions = s3Config.authSchemeProvider().resolveAuthScheme(authSchemeParams);
            //     // modify authSchemeOptions to add more params here
            // };
            // s3Config.authSchemeProvider(customAuthSchemeProvider);
        };

        S3Client client = S3Client.builder()
                                  .addPlugin(customPlugin)
                                  .build();

        //make any API call
        client.putObject(r -> r.bucket("bucket").key("key"), RequestBody.fromString("test"));
    }

    static class CustomS3ExpressAuthScheme implements S3ExpressAuthScheme {
        private final HttpSigner<S3ExpressSessionCredentials> signer;
        private SessionCredentialsHandler sessionCredentialsHandler = new SessionCredentialsHandler();

        CustomS3ExpressAuthScheme(HttpSigner<S3ExpressSessionCredentials> signer) {
            this.signer = signer;
        }

        @Override
        public IdentityProvider<S3ExpressSessionCredentials> identityProvider(IdentityProviders providers) {
            return new CustomIdentityProvider(getCredentialsHandler(), providers.identityProvider(AwsCredentialsIdentity.class));
        }

        // Initialize handler/cache here, singleton logic / synchronization if needed
        private SessionCredentialsHandler getCredentialsHandler() {
            return sessionCredentialsHandler;
        }

        @Override
        public HttpSigner<S3ExpressSessionCredentials> signer() {
            return signer;
        }

        @Override
        public String schemeId() {
            return S3ExpressAuthScheme.SCHEME_ID;
        }

        static class SessionCredentialsHandler {
            S3ExpressSessionCredentials sessionCredentials() {
                return S3ExpressSessionCredentials.create("x", "y", "z");
            }
        }
    }

    static class CustomIdentityProvider implements IdentityProvider<S3ExpressSessionCredentials> {

        private final CustomS3ExpressAuthScheme.SessionCredentialsHandler sessionCredentialsHandler;
        private final IdentityProvider<AwsCredentialsIdentity> baseIdentityProvider;

        CustomIdentityProvider(CustomS3ExpressAuthScheme.SessionCredentialsHandler sessionCredentialsHandler,
                               IdentityProvider<AwsCredentialsIdentity> baseIdentityProvider) {
            this.sessionCredentialsHandler = sessionCredentialsHandler;
            this.baseIdentityProvider = baseIdentityProvider;
        }

        @Override
        public Class<S3ExpressSessionCredentials> identityType() {
            return S3ExpressSessionCredentials.class;
        }

        @Override
        public CompletableFuture<? extends S3ExpressSessionCredentials> resolveIdentity(ResolveIdentityRequest request) {
            String bucket = request.property(S3ExpressAuthSchemeProvider.BUCKET);
            SdkClient client = request.property(SdkIdentityProperty.SDK_CLIENT);

            return baseIdentityProvider.resolveIdentity(request)
                                       .thenApply(identity -> sessionCredentialsHandler.sessionCredentials());
        }
    }

    static class CustomSigner implements HttpSigner<S3ExpressSessionCredentials> {
        private final HttpSigner<S3ExpressSessionCredentials> delegateSigner;
        private final List<String> excludedHeaders;

        CustomSigner(HttpSigner<S3ExpressSessionCredentials> delegateSigner, List<String> excludedHeaders) {
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

            SignedRequest signedRequest = delegateSigner.sign((SignRequest<? extends S3ExpressSessionCredentials>) modifiedRequestToSign);

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

}
