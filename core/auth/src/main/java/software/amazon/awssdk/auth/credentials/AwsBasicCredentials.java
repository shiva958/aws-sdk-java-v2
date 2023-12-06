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

package software.amazon.awssdk.auth.credentials;

import static software.amazon.awssdk.utils.StringUtils.trimToNull;

import java.util.Objects;
import java.util.Optional;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.utils.ToString;
import software.amazon.awssdk.utils.builder.SdkBuilder;

/**
 * Provides access to the AWS credentials used for accessing services: AWS access key ID and secret access key. These
 * credentials are used to securely sign requests to services (e.g., AWS services) that use them for authentication.
 *
 * <p>For more details on AWS access keys, see:
 * <a href="https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys">
 * https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html#access-keys-and-secret-access-keys</a></p>
 *
 * @see AwsCredentialsProvider
 */
@Immutable
@SdkPublicApi
public final class AwsBasicCredentials implements AwsCredentials {
    /**
     * A set of AWS credentials without an access key or secret access key, indicating that anonymous access should be used.
     *
     * This should be accessed via {@link AnonymousCredentialsProvider#resolveCredentials()}.
     */
    @SdkInternalApi
    static final AwsBasicCredentials ANONYMOUS_CREDENTIALS = builder().build();

    private final String accessKeyId;
    private final String secretAccessKey;
    private final String credentialScope;

    private AwsBasicCredentials(DefaultBuilder builder) {
        this.accessKeyId = trimToNull(builder.accessKeyId);
        this.secretAccessKey = trimToNull(builder.secretAccessKey);
        this.credentialScope = builder.credentialScope;
    }

    /**
     * Returns a builder for this object.
     */
    public static Builder builder() {
        return new DefaultBuilder();
    }

    /**
     * Constructs a new credentials object, with the specified AWS access key and AWS secret key.
     *
     * @param accessKeyId     The AWS access key, used to identify the user interacting with AWS.
     * @param secretAccessKey The AWS secret access key, used to authenticate the user interacting with AWS.
     */
    public static AwsBasicCredentials create(String accessKeyId, String secretAccessKey) {
        return builder().accessKeyId(accessKeyId)
                        .secretAccessKey(secretAccessKey)
                        .build();
    }

    /**
     * Retrieve the AWS access key, used to identify the user interacting with AWS.
     */
    @Override
    public String accessKeyId() {
        return accessKeyId;
    }

    /**
     * Retrieve the AWS secret access key, used to authenticate the user interacting with AWS.
     */
    @Override
    public String secretAccessKey() {
        return secretAccessKey;
    }

    /**
     * Retrieve the AWS region of the single-region account, if it exists. Otherwise, returns empty {@link Optional}.
     */
    @Override
    public Optional<String> credentialScope() {
        return Optional.ofNullable(credentialScope);
    }

    @Override
    public String toString() {
        return ToString.builder("AwsCredentials")
                       .add("accessKeyId", accessKeyId)
                       .add("credentialScope", credentialScope)
                       .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AwsBasicCredentials that = (AwsBasicCredentials) o;
        return Objects.equals(accessKeyId, that.accessKeyId) &&
               Objects.equals(secretAccessKey, that.secretAccessKey) &&
               Objects.equals(credentialScope, that.credentialScope().orElse(null));
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = 31 * hashCode + Objects.hashCode(accessKeyId());
        hashCode = 31 * hashCode + Objects.hashCode(secretAccessKey());
        hashCode = 31 * hashCode + Objects.hashCode(credentialScope());
        return hashCode;
    }

    /**
     * A builder for creating an instance of {@link AwsBasicCredentials}. This can be created with the static
     * {@link #builder()} method.
     */
    public interface Builder extends SdkBuilder<AwsBasicCredentials.Builder, AwsBasicCredentials> {

        /**
         * The AWS access key, used to identify the user interacting with services.
         */
        Builder accessKeyId(String accessKeyId);

        /**
         * The AWS secret access key, used to authenticate the user interacting with services.
         */
        Builder secretAccessKey(String secretAccessKey);

        /**
         * The AWS region of the single-region account.
         */
        Builder credentialScope(String credentialScope);
    }

    private static final class DefaultBuilder implements Builder {
        private String accessKeyId;
        private String secretAccessKey;
        private String credentialScope;

        @Override
        public Builder accessKeyId(String accessKeyId) {
            this.accessKeyId = accessKeyId;
            return this;
        }

        @Override
        public Builder secretAccessKey(String secretAccessKey) {
            this.secretAccessKey = secretAccessKey;
            return this;
        }

        @Override
        public Builder credentialScope(String credentialScope) {
            this.credentialScope = credentialScope;
            return this;
        }

        @Override
        public AwsBasicCredentials build() {
            return new AwsBasicCredentials(this);
        }
    }
}
