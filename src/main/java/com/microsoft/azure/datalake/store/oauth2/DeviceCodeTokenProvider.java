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
 *
 * Enables interactive login in non-browser based contexts. Displays a message asking the user to login using
 * the browser, using the provided link and code.
 *
 *
 */
public class DeviceCodeTokenProvider extends AccessTokenProvider {
    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.oauth2.DeviceCodeTokenProvider");
    private RefreshTokenBasedTokenProvider tokenProviderInternal = null;
    private String refreshTokenString = null;

    /**
     * Prompts user to log in and constructs a tokenProvider based on the refresh token obtained from the login.
     *
     * @param appId the app ID whose name Azure AD will display on the login screen
     * @throws IOException in case of errors
     */

    public DeviceCodeTokenProvider(String appId) throws IOException {
        this(appId, null);
    }

    /**
     * Prompts user to log in and constructs a tokenProvider based on the refresh token obtained from the login.
     *
     * @param appId the app ID whose name Azure AD will display on the login screen. Can be null.
     *              If not provided, a default appId will be used.
     * @param callback callback that can display the message to the user on how to login. Can be null.
     *                 If not provided, the default callback will be used which diplays the message on standard output.
     * @throws IOException in case of errors
     */
    public DeviceCodeTokenProvider(String appId, DeviceCodeCallback callback) throws IOException {
        if (appId == null || appId.trim().length() == 0) throw new IllegalArgumentException("appId is required");
        if (callback == null) callback = DeviceCodeCallback.getDefaultInstance();

        RefreshTokenInfo token = DeviceCodeTokenProviderHelper.getRefreshToken(appId, callback);
        refreshTokenString = token.refreshToken;
        tokenProviderInternal = new RefreshTokenBasedTokenProvider(null, token);
    }

    @Override
    protected AzureADToken refreshToken() throws IOException {
        return tokenProviderInternal.refreshToken();
    }

    public String getRefreshToken() {
        return refreshTokenString;
    }

}
