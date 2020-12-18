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
    private final String authEndpoint, clientId, refreshToken;

    /**
     * constructs a token provider based on the refresh token provided
     *
     * @param refreshToken the refresh token
     */
    public RefreshTokenBasedTokenProvider(String refreshToken) {
        this.authEndpoint = null;
        this.clientId = null;
        this.refreshToken = refreshToken;
    }

    /**
     * constructs a token provider based on the refresh token provided
     *
     * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
     * @param refreshToken the refresh token
     */
    public RefreshTokenBasedTokenProvider(String clientId, String refreshToken) {
        this.authEndpoint = null;
        this.clientId = clientId;
        this.refreshToken = refreshToken;
    }

    /**
     * constructs a token provider based on the refresh token provided
     *
     * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
     * @param refreshToken the refresh token
     */
    public RefreshTokenBasedTokenProvider(String clientId, RefreshTokenInfo refreshToken) {
        this.authEndpoint = null;
        this.clientId = clientId;
        this.refreshToken = refreshToken.refreshToken;
        if (refreshToken.accessToken != null &&
                !refreshToken.accessToken.equals("") &&
                refreshToken.accessTokenExpiry != null) {
            this.token = new AzureADToken();
            this.token.accessToken = refreshToken.accessToken;
            this.token.expiry = refreshToken.accessTokenExpiry;
        }
    }

    /**
     * constructs a token provider based on the refresh token provided
     *
     * @param authEndpoint the OAuth 2.0 token endpoint associated with the user's directory
     *                     (obtain from Active Directory configuration)
     * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
     * @param refreshToken the refresh token
     */
    public RefreshTokenBasedTokenProvider(String authEndpoint, String clientId, String refreshToken) {
        this.authEndpoint = authEndpoint;
        this.clientId = clientId;
        this.refreshToken = refreshToken;
    }

    @Override
    protected AzureADToken refreshToken() throws IOException {
        log.debug("AADToken: refreshing refresh-token based token");
        if (authEndpoint == null)
            return AzureADAuthenticator.getTokenUsingRefreshToken(clientId, refreshToken);
        return AzureADAuthenticator.getTokenUsingRefreshToken(authEndpoint, clientId, refreshToken);
    }
}
