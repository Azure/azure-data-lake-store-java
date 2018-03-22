/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.oauth2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Provides tokens based on Azure VM's Managed Service Identity
 */
public class MsiTokenProvider extends AccessTokenProvider {

    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.oauth2.MsiTokenProvider");
    private final int localPort = -1;
    private final String tenantGuid;
    private final String clientId;

    /**
     * Constructs a token provider that fetches tokens from the MSI token-service running on an Azure IaaS VM. This
     * only works on an Azure VM with the MSI extansion enabled.
     */
    public MsiTokenProvider() {
        this(null, null);
    }

    /**
     * Constructs a token provider that fetches tokens from the MSI token-service running on an Azure IaaS VM. This
     * only works on an Azure VM with the MSI extansion enabled.
     *
     * @deprecated localPort is not relevant anymore in the new MSI mechanism
     *
     * @param localPort port on localhost for the MSI token service. (the port that was set in the deployment template).
     *                  If 0 or negative number is specified, then assume default port number of 50342.
     */
    @Deprecated
    public MsiTokenProvider(int localPort) {
        this(null, null);
    }

    /**
     * Constructs a token provider that fetches tokens from the MSI token-service running on an Azure IaaS VM. This
     * only works on an Azure VM with the MSI extansion enabled.
     *
     * @deprecated localPort is not relevant anymore in the new MSI mechanism
     *
     * @param localPort port on localhost for the MSI token service. (the port that was set in the deployment template).
     *                  If 0 or negative number is specified, then assume default port number of 50342.
     * @param tenantGuid (optional) AAD Tenant ID {@code guid}. Can be {@code null}.
     */
    @Deprecated
    public MsiTokenProvider(int localPort, String tenantGuid) {
        this(tenantGuid, null);
    }

    public MsiTokenProvider(String tenantGuid, String clientId) {
        this.tenantGuid = tenantGuid;
        this.clientId = clientId;
    }

    @Override
    protected AzureADToken refreshToken() throws IOException {
        log.debug("AADToken: refreshing token from MSI");
        AzureADToken token =  AzureADAuthenticator.getTokenFromMsi(tenantGuid, clientId, false);
        return token;
    }
}