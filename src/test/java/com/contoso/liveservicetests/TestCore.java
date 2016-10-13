/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.contoso.liveservicetests;

import com.contoso.helpers.HelperUtils;
import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;
import com.microsoft.azure.datalake.store.Core;
import com.microsoft.azure.datalake.store.OperationResponse;
import com.microsoft.azure.datalake.store.RequestOptions;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;



public class TestCore {

    final UUID instanceGuid = UUID.randomUUID();
    static Properties prop = null;
    static AzureADToken aadToken = null;
    static String directory = null;
    static ADLStoreClient client = null;
    static boolean testsEnabled = true;


    @BeforeClass
    public static void setup() throws IOException {
        prop = HelperUtils.getProperties();
        aadToken = AzureADAuthenticator.getTokenUsingClientCreds(prop.getProperty("OAuth2TokenUrl"),
                                                                 prop.getProperty("ClientId"),
                                                                 prop.getProperty("ClientSecret") );
        UUID guid = UUID.randomUUID();
        directory = "/" + prop.getProperty("dirName") + "/" + UUID.randomUUID();
        String account = prop.getProperty("StoreAcct") + ".azuredatalakestore.net";
        client = ADLStoreClient.createClient(account, aadToken);
        testsEnabled = Boolean.parseBoolean(prop.getProperty("CoreTestsEnabled", "true"));
    }

    @AfterClass
    public static void teardown() {
    }

    /*
    Create tests
    */

    @Test
    public void createSmallFileWithOverWrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Core.CreateSmallFileWithOverWrite.txt";

        byte [] contents = HelperUtils.getSampleText1();
        putFileContents(filename, contents, true);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file contents should match", Arrays.equals(b, contents));
        assertTrue("file length should match what was written", b.length == contents.length);
    }

    @Test
    public void createSmallFileWithNoOverwrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Core.CreateSmallFileWithNoOverwrite.txt";

        byte [] contents = HelperUtils.getSampleText1();
        putFileContents(filename, contents, false);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file length should match what was written", b.length == contents.length);
        assertTrue("file contents should match", Arrays.equals(b, contents));
    }

    @Test
    public void create4MBFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Core.Create4MBFile.txt";

        byte [] contents = HelperUtils.getRandomBuffer(4 * 1024 * 1024);
        putFileContents(filename, contents, true);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file length should match what was written", b.length == contents.length);
        assertTrue("file contents should match", Arrays.equals(b, contents));
    }

    @Test
    public void create5MBFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Core.Create5MBFile.txt";

        byte [] contents = HelperUtils.getRandomBuffer(11 * 1024 * 1024);
        putFileContents(filename, contents, true);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file length should match what was written", b.length == contents.length);
        assertTrue("file contents should match", Arrays.equals(b, contents));
    }

    @Test
    public void createOverwriteFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Core.CreateOverWriteFile.txt";

        byte[] contents = HelperUtils.getSampleText1();
        putFileContents(filename, contents, true);

        contents = HelperUtils.getSampleText2();
        putFileContents(filename, contents, true);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file length should match what was written", b.length == contents.length);
        assertTrue("file contents should match", Arrays.equals(b, contents));
    }

    @Test(expected = ADLException.class)
    public void createNoOverwriteFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Core.CreateNoOverWriteFile.txt";

        byte[] contents = HelperUtils.getSampleText1();
        putFileContents(filename, contents, true);

        // attempt to overwrite the same file, but with overwrite flag as false. Should fail.
        contents = HelperUtils.getSampleText2();
        putFileContents(filename, contents, false);
    }


    private void putFileContents(String filename, byte[] b, boolean overwrite) throws IOException {
        RequestOptions opts = new RequestOptions();
        OperationResponse resp = new OperationResponse();
        Core.create(filename, overwrite, null, b, 0, b.length, null, null, true, true, client, opts, resp);
        if (!resp.successful) throw client.getExceptionFromResponse(resp, "Error creating file " + filename);
    }

    private byte[] getFileContents(String filename, int maxLength) throws IOException {
        byte[] b = new byte[maxLength];
        int count = 0;
        boolean eof = false;

        while (!eof && count<b.length) {
            RequestOptions opts = new RequestOptions();
            OperationResponse resp = new OperationResponse();
            InputStream in = Core.open(filename, count, 0, null, client, opts, resp);
            System.out.format("Open completed. Current count=%d%n", count);
            if (!resp.successful) throw client.getExceptionFromResponse(resp, "Error reading from file " + filename);
            if (resp.httpResponseCode == 403 || resp.httpResponseCode == 416) {
                eof = true;
                continue;
            }
            int bytesRead;
            while ((bytesRead = in.read(b, count, b.length - count)) != -1) {
                count += bytesRead;
                System.out.format("    read: %d, cumulative:%d%n", bytesRead, count);
                if (count >= b.length) break;
            }
            in.close();
        }
        byte[] b2 = Arrays.copyOfRange(b, 0, count);
        return b2;
    }
}
