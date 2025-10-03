/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.contoso.helpers;


import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.microsoft.azure.datalake.store.*;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;

import java.io.*;
import java.net.URI;
import java.security.SecureRandom;
import java.util.Properties;

public class HelperUtils {

    private HelperUtils() {
        throw new AssertionError("Utility class");
    }

    private static Properties prop = null;
    public static Properties getProperties() throws IOException {
        if (prop==null) {
            Properties defaultProps = new Properties();
            defaultProps.load(new FileInputStream("target/test-classes/config.properties"));
            prop = new Properties(defaultProps);
            prop.load(new FileInputStream("target/test-classes/creds.properties"));
        }
        return prop;
    }

    public static TokenCredential buildCredential(Properties prop) {
        String clientId = trimOrNull(prop.getProperty("ClientId"));
        String clientSecret = trimOrNull(prop.getProperty("ClientSecret"));
        String tenantId = trimOrNull(prop.getProperty("TenantId"));
        if (tenantId == null) {
            tenantId = tryParseTenantId(prop.getProperty("OAuth2TokenUrl"));
        }

        if (clientId != null && clientSecret != null && tenantId != null) {
            ClientSecretCredentialBuilder builder = new ClientSecretCredentialBuilder()
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .tenantId(tenantId);
            String authorityHost = trimOrNull(prop.getProperty("AuthorityHost"));
            if (authorityHost == null) {
                authorityHost = tryParseAuthorityHost(prop.getProperty("OAuth2TokenUrl"));
            }
            if (authorityHost != null) {
                builder.authorityHost(authorityHost);
            }
            return builder.build();
        }

        return new DefaultAzureCredentialBuilder().build();
    }

    private static byte[] buf4mb = null;
    public static byte[] getRandom4mbBuffer() {
        if (buf4mb == null) {
            SecureRandom prng = new SecureRandom();
            buf4mb = new byte[4 * 1024 * 1024];
            prng.nextBytes(buf4mb);
        }
        return buf4mb;
    }

    public static void getRandomBytes(byte[] buf) {
        SecureRandom prng = new SecureRandom();
        prng.nextBytes(buf);
    }

    public static byte[] getRandomBuffer(int len) {
        SecureRandom prng = new SecureRandom();
        byte[] b = new byte[len];
        prng.nextBytes(b);
        return b;
    }

    public static void createEmptyFile(ADLStoreClient client, String filename) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialBackoffPolicy();
        OperationResponse resp = new OperationResponse();
        Core.create(filename, true, null, null, 0, 0, null,
                null, true, SyncFlag.CLOSE, client, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Error creating file " + filename);
        }
    }

    public static void createRandomContentFile(ADLStoreClient client, String filename, int contentLength) throws IOException {
        OutputStream ostr = client.createFile(filename, IfExists.OVERWRITE);
        ostr.write(getRandomBuffer(contentLength));
        ostr.close();
    }

    public static void create256BFile(ADLStoreClient client, String filename) throws IOException {
        createRandomContentFile(client, filename, 256);
    }

    public static byte[] getSampleText1() {
        ByteArrayOutputStream b = new ByteArrayOutputStream(1024);
        PrintStream out = new PrintStream(b);
        try {
            for (int i = 1; i <= 10; i++) {
                out.println("This is line #" + i);
                out.format("This is the same line (%d), but using formatted output. %n", i);
            }
            out.close();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
        return b.toByteArray();
        // length of returned array is 742 bytes
    }

    public static byte[] getSampleText2() {
        ByteArrayOutputStream s = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(s);
        out.println("This is a line");
        out.println("This is another line");
        out.println("This is yet another line");
        out.println("This is yet yet another line");
        out.println("This is yet yet yet another line");
        out.println("... and so on, ad infinitum");
        out.println();
        out.close();
        byte[] buf = s.toByteArray();
        return buf;
        // length of returned array is 159 bytes
    }

    private static String trimOrNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static String tryParseTenantId(String oauthUrl) {
        if (oauthUrl == null) {
            return null;
        }
        try {
            URI uri = URI.create(oauthUrl.trim());
            String path = uri.getPath();
            if (path == null) {
                return null;
            }
            String[] segments = path.split("/");
            for (String segment : segments) {
                if (segment != null && !segment.isEmpty()) {
                    if ("oauth2".equalsIgnoreCase(segment) || "token".equalsIgnoreCase(segment)) {
                        continue;
                    }
                    return segment;
                }
            }
        } catch (IllegalArgumentException ignored) {
        }
        return null;
    }

    private static String tryParseAuthorityHost(String oauthUrl) {
        if (oauthUrl == null) {
            return null;
        }
        try {
            URI uri = URI.create(oauthUrl.trim());
            if (uri.getScheme() == null || uri.getHost() == null) {
                return null;
            }
            return uri.getScheme() + "://" + uri.getHost();
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }


}
