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
    private long tokenFetchTime =-1;
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

    /**
     * Checks if the token is about to expire as per base expiry logic. Otherwise try to expire every 1 hour
     *
     *
     * @return true if the token is expiring in next 5 minutes
     */
    @Override
    protected boolean isTokenAboutToExpire() {
        if( super.isTokenAboutToExpire()){
            return true;
        }
        if (tokenFetchTime == -1){
            return true;
        }
        long offset = ONE_HOUR;
        if ((tokenFetchTime +offset) < System.currentTimeMillis()) {
            log.debug("MSIToken: token renewing : " + offset + " milliseconds window");
            return true;
        }

        return false;
    }

    @Override
    protected AzureADToken refreshToken() throws IOException {
        log.debug("AADToken: refreshing token from MSI with expiry");
        AzureADToken newToken =  AzureADAuthenticator.getTokenFromMsi(tenantGuid, clientId, false);
        tokenFetchTime=System.currentTimeMillis();
        return newToken;
    }
    private static final long ONE_HOUR = 3600 * 1000; // 5 minutes in milliseconds
}