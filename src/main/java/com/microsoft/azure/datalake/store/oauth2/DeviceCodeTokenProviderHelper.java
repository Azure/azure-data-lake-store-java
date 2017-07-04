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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;

/**
 * Internal use. Contains device code info.
 */
class DeviceCodeInfo {
    String usercode;
    String verificationUrl;
    String message;
    String devicecode;
    int pollingInterval;
    Date expiry;
    String clientId;
}


/**
 * Internal use. Methods to obtain the refresh token using interactive login (using device code AAD flow).
 *
 */
class DeviceCodeTokenProviderHelper {

    /*
    AAD .net sample: https://azure.microsoft.com/en-us/resources/samples/active-directory-dotnet-deviceprofile/
    How this stuff works: https://developers.google.com/identity/protocols/OAuth2ForDevices?hl=en
    */

    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.oauth2.DeviceCodeTokenProvider");
    private static final String defaultAppId = "c8964590-6116-42e6-8a29-ec0865dff3d5";
    private static final String resource = AzureADAuthenticator.resource;
    private static final String deviceCodeUrl = "https://login.microsoftonline.com/common/oauth2/devicecode";
    private static final String tokenUrl = "https://login.microsoftonline.com/common/oauth2/token";

    public static RefreshTokenInfo getRefreshToken(String appId, DeviceCodeCallback callback) throws IOException {
        if (appId == null) appId = defaultAppId;
        if (callback == null) callback = DeviceCodeCallback.getDefaultInstance();

        DeviceCodeInfo dcInfo = getDeviceCodeInfo(appId);
        log.debug("AADToken: obtained device code, prompting user to login through browser");
        callback.showDeviceCodeMessage(dcInfo);
        RefreshTokenInfo token = getTokenFromDeviceCode(dcInfo);
        log.debug("AADToken: obtained refresh token from device-code based user login");
        return token;
    }


    private static DeviceCodeInfo getDeviceCodeInfo(String appId) throws IOException {

        QueryParams qp = new QueryParams();
        qp.add("resource", resource);
        qp.add("client_id", appId);
        String queryString = qp.serialize();

        URL url = new URL(deviceCodeUrl + "?" + queryString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        DeviceCodeInfo dcInfo = new DeviceCodeInfo();
        dcInfo.clientId = appId;
        int httpResponseCode = conn.getResponseCode();
        if (httpResponseCode == 200) {
            InputStream httpResponseStream = conn.getInputStream();
            try {
                int expiryPeriod = 0;

                JsonFactory jf = new JsonFactory();
                JsonParser jp = jf.createParser(httpResponseStream);
                String fieldName, fieldValue;
                jp.nextToken();
                while (jp.hasCurrentToken()) {
                    if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
                        fieldName = jp.getCurrentName();
                        jp.nextToken();  // field value
                        fieldValue = jp.getText();

                        if (fieldName.equals("user_code")) dcInfo.usercode = fieldValue;
                        if (fieldName.equals("device_code")) dcInfo.devicecode = fieldValue;
                        if (fieldName.equals("verification_url")) dcInfo.verificationUrl = fieldValue;
                        if (fieldName.equals("message")) dcInfo.message = fieldValue;
                        if (fieldName.equals("expires_in")) expiryPeriod = Integer.parseInt(fieldValue);
                        if (fieldName.equals("interval")) dcInfo.pollingInterval = Integer.parseInt(fieldValue);
                    }
                    jp.nextToken();
                }
                jp.close();
                long expiry = System.currentTimeMillis();
                expiry = expiry + expiryPeriod * 1000L; // convert expiryPeriod to milliseconds and add
                dcInfo.expiry = new Date(expiry);
            } finally {
                httpResponseStream.close();
            }
        } else {
            String message = "Failed to get device code from AzureAD. Http response: " + httpResponseCode + " " + conn.getResponseMessage();
            log.debug(message);
            throw new IOException(message);
        }
        log.debug("Obtained device code from AAD: " + dcInfo.usercode);
        return dcInfo;
    }

    private static RefreshTokenInfo getTokenFromDeviceCode(final DeviceCodeInfo dcInfo) throws IOException {
        RefreshTokenInfo refreshToken = null;
        int sleepDuration = (dcInfo.pollingInterval + 1) * 1000;
        while (dcInfo.expiry.getTime() > (new Date()).getTime() && refreshToken == null) {
            try {
                Thread.sleep(sleepDuration);
                refreshToken = getTokenInternal(dcInfo.devicecode, dcInfo.clientId);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();   // http://www.ibm.com/developerworks/library/j-jtp05236/
            } catch (Exception ex) {
                log.debug("Exception getting token from device code " + ex.toString());
                throw ex;
            }
        }
        return refreshToken;
    }

    private static RefreshTokenInfo getTokenInternal(final String deviceCode, final String clientId) throws IOException {
        QueryParams qp = new QueryParams();
        qp.add("resource", resource);
        qp.add("client_id", clientId);
        qp.add("grant_type", "device_code");
        qp.add("code", deviceCode);
        String bodyString = qp.serialize();

        URL url = new URL(tokenUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.getOutputStream().write(bodyString.getBytes("UTF-8"));

        RefreshTokenInfo token = new RefreshTokenInfo();
        String tokentype = null;
        String scope = null;
        int httpResponseCode = conn.getResponseCode();
        if (httpResponseCode == 200) {
            InputStream httpResponseStream = conn.getInputStream();
            try {
                int expiryPeriod = 0;
                JsonFactory jf = new JsonFactory();
                JsonParser jp = jf.createParser(httpResponseStream);
                String fieldName, fieldValue;
                jp.nextToken();
                while (jp.hasCurrentToken()) {
                    if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
                        fieldName = jp.getCurrentName();
                        jp.nextToken();  // field value
                        fieldValue = jp.getText();

                        if (fieldName.equals("token_type")) tokentype = fieldValue;
                        if (fieldName.equals("scope")) scope = fieldValue;
                        if (fieldName.equals("expires_in")) expiryPeriod = Integer.parseInt(fieldValue);
                        if (fieldName.equals("access_token")) token.accessToken = fieldValue;
                        if (fieldName.equals("refresh_token")) token.refreshToken = fieldValue;
                    }
                    jp.nextToken();
                }
                jp.close();

                if (!"Bearer".equals(tokentype) || !"user_impersonation".equals(scope) ) {
                    throw new IOException("not sure what kind of token we got");
                }

                long expiry = System.currentTimeMillis();
                expiry = expiry + expiryPeriod * 1000L; // convert expiryPeriod to milliseconds and add
                token.accessTokenExpiry = new Date(expiry);
                return token;
            } catch (Exception ex) {
                log.debug("Exception retrieving token from AAD response" + ex.toString());
                throw ex;
            } finally {
                httpResponseStream.close();
            }
        } else if (httpResponseCode == 400) {
            InputStream httpResponseStream = conn.getErrorStream();
            try {
                String error = null;

                JsonFactory jf = new JsonFactory();
                JsonParser jp = jf.createParser(httpResponseStream);
                String fieldName, fieldValue;
                jp.nextToken();
                while (jp.hasCurrentToken()) {
                    if (jp.getCurrentToken() == JsonToken.FIELD_NAME) {
                        fieldName = jp.getCurrentName();
                        jp.nextToken();  // field value
                        fieldValue = jp.getText();

                        if (fieldName.equals("error")) error = fieldValue;
                    }
                    jp.nextToken();
                }
                jp.close();

                if (!"authorization_pending".equals(error)) {
                    String message = "Failed to acquire token from AzureAD. Http response: " + httpResponseCode + " Error: " + error;
                    log.debug(message);
                    throw new IOException(message);
                } else {
                    log.debug("polled AAD for token, got authorization_pending (still waiting for user to complete login)");
                }
            } finally {
                httpResponseStream.close();
            }
        } else {
            String message = "Failed to acquire token from AzureAD. Http response: " + httpResponseCode + " " + conn.getResponseMessage();
            log.debug(message);
            throw new IOException(message);
        }
        return null;
    }

    private static void printDeviceCodeInfo(DeviceCodeInfo dcInfo) {
        System.out.println("UserCode: " + dcInfo.usercode);
        System.out.println("VerificationUrl: " + dcInfo.verificationUrl);
        System.out.println("Polling Interval: " + dcInfo.devicecode);
        System.out.println("Expires: " + dcInfo.expiry);
        System.out.println("Message: " + dcInfo.message);
        System.out.println("Devicecode: " + dcInfo.devicecode);
        System.out.println();
    }
}
