/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.oauth2;


import java.util.Date;

/**
 * Information about the refresh token, and the associated access token
 *
 */
public class RefreshTokenInfo {
    public String accessToken;
    public String refreshToken;
    public Date accessTokenExpiry;
}