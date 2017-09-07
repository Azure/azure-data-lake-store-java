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
    private final int localPort;
    private final String tenantGuid;

    /**
     * Constructs a token provider that fetches tokens from the MSI token-service running on an Azure IaaS VM. This
     * only works on an Azure VM with the MSI extansion enabled.
     */
    public MsiTokenProvider() {
        this(-1, null);
    }

    /**
     * Constructs a token provider that fetches tokens from the MSI token-service running on an Azure IaaS VM. This
     * only works on an Azure VM with the MSI extansion enabled.
     * @param localPort port on localhost for the MSI token service. (the port that was set in the deployment template).
     *                  If 0 or negative number is specified, then assume default port number of 50342.
     */
    public MsiTokenProvider(int localPort) {
        this(localPort, null);
    }

    /**
     * Constructs a token provider that fetches tokens from the MSI token-service running on an Azure IaaS VM. This
     * only works on an Azure VM with the MSI extansion enabled.
     * @param localPort port on localhost for the MSI token service. (the port that was set in the deployment template).
     *                  If 0 or negative number is specified, then assume default port number of 50342.
     * @param tenantGuid (optional) AAD Tenant ID {@code guid}. Can be {@code null}.
     */
    public MsiTokenProvider(int localPort, String tenantGuid) {
        this.localPort = localPort;
        this.tenantGuid = tenantGuid;
    }

    @Override
    protected AzureADToken refreshToken() throws IOException {
        log.debug("AADToken: refreshing token from MSI");
        return AzureADAuthenticator.getTokenFromMsi(localPort, tenantGuid);
    }
}