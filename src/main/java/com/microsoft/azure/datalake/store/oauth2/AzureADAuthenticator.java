/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.oauth2;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.microsoft.azure.datalake.store.QueryParams;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicyforMSI;
import com.microsoft.azure.datalake.store.retrypolicies.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Hashtable;
import java.util.UUID;

/**
 * This class provides convenience methods to obtain AAD tokens. While convenient, it is not necessary to
 * use these methods to obtain the tokens. Customers can use any other method (e.g., using the adal4j client)
 * to obtain tokens.
 */

public class AzureADAuthenticator {

    private static final Logger log = LoggerFactory.getLogger(AzureADAuthenticator.class.getName());
    static final String resource = "https://datalake.azure.net/";

    /**
     * gets Azure Active Directory token using the user ID and password of a service principal (that is, Web App
     * in Azure Active Directory).
     * <P>
     * Azure Active Directory allows users to set up a web app as a service principal. Users can optionally
     * obtain service principal keys from AAD. This method gets a token using a service principal's client ID
     * and keys. In addition, it needs the token endpoint associated with the user's directory.
     * </P>
     *
     *
     * @param authEndpoint the OAuth 2.0 token endpoint associated with the user's directory
     *                     (obtain from Active Directory configuration)
     * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
     * @param clientSecret the secret key of the client web app
     * @return {@link AzureADToken} obtained using the creds
     * @throws IOException throws IOException if there is a failure in connecting to Azure AD
     */
    public static AzureADToken getTokenUsingClientCreds(String authEndpoint, String clientId, String clientSecret)
            throws IOException
    {
        QueryParams qp = new QueryParams();

        qp.add("resource", resource);
        qp.add("grant_type","client_credentials");
        qp.add("client_id", clientId);
        qp.add("client_secret", clientSecret);
        log.debug("AADToken: starting to fetch token using client creds for client ID " + clientId );

        return getTokenCall(authEndpoint, qp.serialize(), null, null, null);
    }


    /**
     * Gets AAD token from the local virtual machine's VM extension. This only works on an Azure VM with MSI extension
     * enabled.
     *
     * @deprecated  Deprecated, use the other overloads instead. With the change to the way MSI is done in
     * Azure Active Directory, the parameters on this call (localPort) are not relevant anymore.
     *
     * @param localPort port at which the MSI extension is running. If 0 or negative number is specified, then assume
     *                  default port number of 50342.
     * @param tenantGuid (optional) The guid of the AAD tenant. Can be {@code null}.
     * @return {@link AzureADToken} obtained using the creds
     * @throws IOException throws IOException if there is a failure in obtaining the token
     */
    @Deprecated
    public static AzureADToken getTokenFromMsi(int localPort, String tenantGuid) throws IOException {
        return getTokenFromMsi(tenantGuid, null, false);
    }

    /**
     * Gets AAD token from the local virtual machine's VM extension. This only works on an Azure VM with MSI extension
     * enabled.
     *
     * @param tenantGuid (optional) The guid of the AAD tenant. Can be {@code null}.
     * @param clientId (optional) The clientId guid of the MSI service principal to use. Can be {@code null}.
     * @param bypassCache {@code boolean} specifying whether a cached token is acceptable or a fresh token
     *                                   request should me made to AAD
     * @return {@link AzureADToken} obtained using the creds
     * @throws IOException throws IOException if there is a failure in obtaining the token
     */
    public static AzureADToken getTokenFromMsi(String tenantGuid, String clientId, boolean bypassCache) throws IOException {
        String authEndpoint  = "http://169.254.169.254/metadata/identity/oauth2/token";

        QueryParams qp = new QueryParams();
        qp.add("api-version", "2018-02-01");
        qp.add("resource", resource);


        if (tenantGuid != null && tenantGuid.length() > 0) {
            String authority = "https://login.microsoftonline.com/" + tenantGuid;
            qp.add("authority", authority);
        }

        if (clientId != null && clientId.length() > 0) {
            qp.add("client_id", clientId);
        }

        if (bypassCache) {
            qp.add("bypass_cache", "true");
        }

        Hashtable<String, String> headers = new Hashtable<String, String>();
        headers.put("Metadata", "true");

        RetryPolicy retryPolicy = new ExponentialBackoffPolicyforMSI(3, 1000, 2);

        log.debug("AADToken: starting to fetch token using MSI");
        return getTokenCall(authEndpoint, qp.serialize(), headers, "GET", retryPolicy);
    }

    /**
     * gets Azure Active Directory token using refresh token
     *
     * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
     * @param refreshToken the refresh token
     * @return {@link AzureADToken} obtained using the refresh token
     * @throws IOException throws IOException if there is a failure in connecting to Azure AD
     */
    public static AzureADToken getTokenUsingRefreshToken(String clientId, String refreshToken)
            throws IOException
    {
        String authEndpoint = "https://login.microsoftonline.com/Common/oauth2/token";

        QueryParams qp = new QueryParams();
        qp.add("grant_type", "refresh_token");
        qp.add("refresh_token", refreshToken);
        if (clientId != null) qp.add("client_id", clientId);
        log.debug("AADToken: starting to fetch token using refresh token for client ID " + clientId );

        return getTokenCall(authEndpoint, qp.serialize(), null, null, null);
    }

    /**
     * gets Azure Active Directory token using the user's username and password. This only
     * works if the identity can be authenticated directly by microsoftonline.com. It will likely
     * not work if the domain is federated and/or multi-factor authentication or other form of
     * strong authentication is configured for the user.
     * <P>
     * @deprecated
     * Due to security concerns with user ID and password,this auth method is deprecated. Please use
     * device code authentication instead for interactive user-based authentication.
     * </P>
     * @param clientId the client ID (GUID) of the client web app obtained from Azure Active Directory configuration
     * @param username the user name of the user
     * @param password the password of the user
     * @return {@link AzureADToken} obtained using the user's creds
     * @throws IOException throws IOException if there is a failure in connecting to Azure AD
     */
    @Deprecated
    public static AzureADToken getTokenUsingUserCreds(String clientId, String username, String password)
            throws IOException
    {
        String authEndpoint = "https://login.microsoftonline.com/Common/oauth2/token";

        QueryParams qp = new QueryParams();
        qp.add("grant_type", "password");
        qp.add("resource", resource);
        qp.add("scope", "openid");
        qp.add("client_id", clientId);
        qp.add("username",username);
        qp.add("password",password);
        log.debug("AADToken: starting to fetch token using username for user " + username );

        return getTokenCall(authEndpoint, qp.serialize(), null, null, null);
    }

    private static class HttpException extends IOException {
        public int httpErrorCode;
        public String requestId;

        public HttpException(int httpErrorCode, String requestId, String message) {
            super(message);
            this.httpErrorCode = httpErrorCode;
            this.requestId = requestId;
        }
    }

    private static AzureADToken getTokenCall(String authEndpoint, String body, Hashtable<String, String> headers, String httpMethod, RetryPolicy retryPolicy)
            throws IOException {
        AzureADToken token = null;

        RetryPolicy retryPolicyUsed;
        retryPolicyUsed = (retryPolicy != null) ?
                retryPolicy : new ExponentialBackoffPolicy(3, 0, 1000, 2);

        int httperror = 0;
        String requestId;
        String httpExceptionMessage = null;
        IOException ex = null;
        boolean succeeded = false;
        String clientRequestId = UUID.randomUUID().toString();
        int retryCount = 0;
        if(headers == null){
            headers = new Hashtable<String, String>();
        }
        do {
            httperror = 0;
            ex = null;
            try {
                headers.put("client-request-id", clientRequestId + "."+retryCount);
                token = getTokenSingleCall(authEndpoint, body, headers, httpMethod);
                retryCount++;
            } catch (HttpException e) {
                httperror = e.httpErrorCode;
                requestId = e.requestId;
                httpExceptionMessage = e.getMessage();
            }  catch (IOException e) {
                ex = e;
            }
            succeeded = ((httperror == 0) && (ex == null));
        } while (!succeeded && retryPolicyUsed.shouldRetry(httperror, ex));
        if (!succeeded) {
            if (ex != null) throw ex;
            if (httperror!=0) throw new IOException(httpExceptionMessage);
        }
        return token;
    }

    private static AzureADToken getTokenSingleCall(String authEndpoint, String payload, Hashtable<String, String> headers, String httpMethod)
            throws IOException {

        AzureADToken token = null;
        HttpURLConnection conn = null;
        String urlString = authEndpoint;

        httpMethod = (httpMethod == null) ? "POST" : httpMethod;
        if (httpMethod.equals("GET")) {
            urlString = urlString + "?" + payload;
        }
        long startTime = System.nanoTime();
        long totalTime = 0;

        try {
            URL url = new URL(urlString);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(httpMethod);
            conn.setReadTimeout(30000);
            conn.setConnectTimeout(30000);

            if (headers != null && headers.size() > 0) {
                for (String name : headers.keySet()) {
                    conn.setRequestProperty(name, headers.get(name));
                }
            }

            conn.setRequestProperty("Connection", "close");

            if (httpMethod.equals("POST")) {
                conn.setDoOutput(true);
                conn.getOutputStream().write(payload.getBytes("UTF-8"));
            }

            int httpResponseCode = conn.getResponseCode();
            String requestId = conn.getHeaderField("x-ms-request-id");
            String responseContentType = conn.getHeaderField("Content-Type");
            long responseContentLength = conn.getHeaderFieldLong("Content-Length", 0);
            requestId = requestId == null ? "" : requestId;

            if (httpResponseCode == 200 && responseContentType.startsWith("application/json")) {
                InputStream httpResponseStream = conn.getInputStream();
                token = parseTokenFromStream(httpResponseStream);

                if (log.isDebugEnabled()) {
                    totalTime = System.nanoTime() - startTime;
                    String logMessage =
                            "AADToken: HTTP connection succeeded for getting token from AzureAD. Http response: "
                                    + httpResponseCode + " " + conn.getResponseMessage()
                                    + " Content-Type: " + responseContentType
                                    + " Content-Length: " + responseContentLength
                                    + " Request ID: " + requestId
                                    + " Client Request Id: " + headers.get("client-request-id")
                                    + " Latency(ns) : " + totalTime;
                    log.debug(logMessage);
                }
            } else {
                String responseBody = new String(readAllBytes(conn.getInputStream()), StandardCharsets.UTF_8);
                totalTime = System.nanoTime() - startTime;
                String proxies = "none";
                String httpProxy=System.getProperty("http.proxy");
                String httpsProxy=System.getProperty("https.proxy");
                if (httpProxy!=null || httpsProxy!=null) {
                    proxies = "http:" + httpProxy + ";https:" + httpsProxy;
                }
                String logMessage =
                          "AADToken: HTTP connection failed for getting token from AzureAD. Http response: "
                        + httpResponseCode + " " + conn.getResponseMessage()
                        + " Content-Type: " + responseContentType
                        + " Content-Length: " + responseContentLength
                        + " Request ID: " + requestId.toString()
                        + " Client Request Id: " + headers.get("client-request-id")
                        + " Latency(ns) : " + totalTime
                        + " Proxies: " + proxies
                        + " First 1K of Body: " + responseBody;
                log.debug(logMessage);
                throw new HttpException(httpResponseCode, requestId, logMessage);
            }
        } catch(IOException e) {
            totalTime = System.nanoTime() - startTime;
            String logMessage =
                    "AADToken: HTTP connection failed for getting token from AzureAD due to timeout. "
                    + " Client Request Id :" + headers.get("client-request-id")
                    + " Latency(ns) : " + totalTime;
            log.debug(logMessage);
            throw new IOException(logMessage, e);
        }
        finally {
            if (conn != null) conn.disconnect();
        }
        return token;
    }

    private static AzureADToken parseTokenFromStream(InputStream httpResponseStream) throws IOException {
        AzureADToken token = new AzureADToken();
        try {
            int expiryPeriodRelative = 0;
            long expiryPeriodActual = -1;
            JsonFactory jf = new JsonFactory();
            JsonParser jp = jf.createParser(httpResponseStream);
            String fieldName, fieldValue;
            jp.nextToken();
            while (jp.hasCurrentToken()) {
                if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
                    fieldName = jp.getCurrentName();
                    jp.nextToken();  // field value
                    fieldValue = jp.getText();

                    if (fieldName.equals("access_token")) token.accessToken = fieldValue;
                    if (fieldName.equals("expires_in")) expiryPeriodRelative = Integer.parseInt(fieldValue);
                    if (fieldName.equals("expires_on")) expiryPeriodActual = Integer.parseInt(fieldValue);
                }
                jp.nextToken();
            }
            jp.close();
            boolean expiresOn = false;
            if(expiryPeriodActual>0) {
                // convert expiryPeriodActual to milliseconds
                token.expiry = new Date(expiryPeriodActual*1000);
                expiresOn=true;
            }
            else {
                long expiry = System.currentTimeMillis();
                expiry = expiry + expiryPeriodRelative * 1000L; // convert expiryPeriod to milliseconds and add
                token.expiry = new Date(expiry);
            }
            log.debug("AADToken: fetched token with expiry " + token.expiry.toString() + " expiresOn passed: "+expiresOn);
        } catch (Exception ex) {
            log.debug("AADToken: got exception when parsing json token " + ex.toString());
            throw ex;
        } finally {
            httpResponseStream.close();
        }
        return token;
    }

    private static byte[] readAllBytes(InputStream inputStream) throws IOException {
        final int bufLen = 4 * 0x400; // 4KB
        byte[] buf = new byte[bufLen];
        int readLen;
        IOException exception = null;

        try {
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                while ((readLen = inputStream.read(buf, 0, bufLen)) != -1)
                    outputStream.write(buf, 0, readLen);

                return outputStream.toByteArray();
            }
        } catch (IOException e) {
            exception = e;
            throw e;
        } finally {
            if (exception == null) inputStream.close();
            else try {
                inputStream.close();
            } catch (IOException e) {
                exception.addSuppressed(e);
            }
        }
    }
}



