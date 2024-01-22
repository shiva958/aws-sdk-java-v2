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

package software.amazon.awssdk.codegen.model.config.customization;

import java.util.List;

public class AuthSchemeConfig {
    private List<AuthScheme> authSchemes;
    private List<OperationAuthSchemeConfig> operations;

    public List<AuthScheme> getAuthSchemes() {
        return authSchemes;
    }

    public void setAuthSchemes(List<AuthScheme> authSchemes) {
        this.authSchemes = authSchemes;
    }

    public List<OperationAuthSchemeConfig> getOperations() {
        return operations;
    }

    public void setOperations(List<OperationAuthSchemeConfig> operations) {
        this.operations = operations;
    }
}
