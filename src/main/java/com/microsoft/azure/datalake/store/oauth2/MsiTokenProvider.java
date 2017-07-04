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
     * Constructs a token provider that fetches tokens from the MSI token-service running on an Azure IaaS VM
     * @param localPort port on localhost for the MSI token service. (the port that was set in the deployment template)
     * @param tenantGuid AAD Tenant ID ({@code guid})
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