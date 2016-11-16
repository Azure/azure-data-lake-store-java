package com.microsoft.azure.datalake.store.oauth2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Provides tokens based on username and password
 */
public class UserPasswordTokenProvider extends AccessTokenProvider {

    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.oauth2.UserPasswordTokenProvider");
    private final String clientId, username, password;

    /**
     * constructs a token provider based on supplied credentials.
     *
     * @param username the username
     * @param clientId the client ID (GUID) obtained from Azure Active Directory configuration
     * @param password the password
     */
    public UserPasswordTokenProvider(String clientId, String username, String password) {
        this.clientId = clientId;
        this.username = username;
        this.password = password;
    }

    @Override
    protected AzureADToken refreshToken() throws IOException {
        log.debug("AADToken: refreshing user-password based token");
        return AzureADAuthenticator.getTokenUsingClientCreds(clientId, username, password);
    }
}
