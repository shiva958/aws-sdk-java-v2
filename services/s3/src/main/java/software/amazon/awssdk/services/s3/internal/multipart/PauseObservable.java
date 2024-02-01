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

import software.amazon.awssdk.annotations.SdkInternalApi;

@SdkInternalApi
public class PauseObservable {

    // TODO - should we have S3Token here, to pass when resuming?
    // could then possibly get rid of S3ResumeToken attribute?
    // or just uplaodID String and get rid of S3ResumeToken object

    private volatile UploadWithKnownContentLengthHelper.KnownContentLengthAsyncRequestBodySubscriber subscriber;

    public void setSubscriber(UploadWithKnownContentLengthHelper.KnownContentLengthAsyncRequestBodySubscriber subscriber) {
        this.subscriber = subscriber;
    }

    public S3ResumeToken pause() {
        return subscriber.pause();
    }
}
