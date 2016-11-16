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
 * Provides tokens based on refresh token
 */
public class RefreshTokenBasedTokenProvider extends AccessTokenProvider {

    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.oauth2.RefreshTokenBasedTokenProvider");
    private final String clientId, refreshToken;

    /**
     * constructs a token provider based on the refresh token provided
     *
     * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
     * @param refreshToken the refresh token
     */
    public RefreshTokenBasedTokenProvider(String clientId, String refreshToken) {
        this.clientId = clientId;
        this.refreshToken = refreshToken;
    }

    @Override
    protected AzureADToken refreshToken() throws IOException {
        log.debug("AADToken: refreshing refresh-token based token");
        return AzureADAuthenticator.getTokenUsingRefreshToken(clientId, refreshToken);
    }
}
