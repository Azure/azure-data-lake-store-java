/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.oauth2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * Returns an Azure Active Directory token when requested. The provider can cache the token if it has already
 * retrieved one. If it does, then the provider is responsible for checking expiry and refreshing as needed.
 * <P>
 * In other words, this is is a token cache that fetches tokens when requested, if the cached token has expired.
 * </P>
 */
public abstract class AccessTokenProvider {

    protected AzureADToken token;
    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider");

    /**
     * returns the {@link AzureADToken} cached (or retrieved) by this instance.
     *
     * @return {@link AzureADToken} containing the access token
     * @throws IOException if there is an error fetching the token
     */
    public synchronized AzureADToken getToken() throws IOException {
        if (isTokenAboutToExpire()) {
            log.debug("AAD Token is missing or expired: Calling refresh-token from abstract base class");
            token = refreshToken();
        }
        return token;
    }

    /**
     * the method to fetch the access token. Derived classes should override this method to
     * actually get the token from Azure Active Directory.
     * <P>
     * This method will be called initially, and then once when the token is about to expire.
     * </P>
     *
     *
     * @return {@link AzureADToken} containing the access token
     * @throws IOException if there is an error fetching the token
     */
    protected abstract AzureADToken refreshToken() throws IOException;

    /**
     * Checks if the token is about to expire in the next 5 minutes. The 5 minute allowance is to
     * allow for clock skew and also to allow for token to be refreshed in that much time.
     *
     *
     * @return true if the token is expiring in next 5 minutes
     */
    protected boolean isTokenAboutToExpire() {
        if (token==null) {
            log.debug("AADToken: no token. Returning expiring=true");
            return true;   // no token should have same response as expired token
        }
        if (token.expiry == null) {
            log.debug("AADToken: no token expiry set. Returning expiring=true");
            return true; // if don't know expiry then assume expired (should not happen with a correctly implemented token)
        }
        long offset = FIVE_MINUTES;
        long approximatelyNow = System.currentTimeMillis() + offset;   // allow x minutes for clock skew, depends on type of provider
        boolean expiring = (token.expiry.getTime() < approximatelyNow);
        if (expiring) {
            log.debug("AADToken: token expiring: " + token.expiry.toString() + " : " + offset + " milliseconds window: " + new Date(approximatelyNow).toString());
        }

        return expiring;
    }
    private static final long FIVE_MINUTES = 300 * 1000; // 5 minutes in milliseconds
}
