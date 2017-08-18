/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.contoso.liveservicetests;

import com.contoso.helpers.HelperUtils;
import com.microsoft.azure.datalake.store.*;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;

import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
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
        System.out.println("Running createSmallFileWithOverWrite");

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
        System.out.println("Running createSmallFileWithNoOverwrite");

        byte [] contents = HelperUtils.getSampleText1();
        putFileContents(filename, contents, false);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file length should match what was written", b.length == contents.length);
        assertTrue("file contents should match", Arrays.equals(b, contents));
    }


    @Test
    public void createEmptyFileWithConcurrentAppend() throws IOException {
        Assume.assumeTrue(false);  // pending change to server behavior
        String filename = directory + "/" + "Core.createEmptyFileWithConcurrentAppend.txt";
        System.out.println("Running createEmptyFileWithConcurrentAppend");

        byte [] contents = null;
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        OperationResponse resp = new OperationResponse();
        Core.concurrentAppend(filename, contents, 0, 0, true,
                client, null, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Error in ConcurrentAppend with null content " + filename);
        }

        DirectoryEntry de = dir(filename);
        assertTrue("File type should be FILE", de.type == DirectoryEntryType.FILE);
        assertTrue("File length in DirectoryEntry should be 0 for null-content file", de.length == 0);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file length should be 0 for null content", b.length == 0);
    }


    @Test
    public void create0LengthFileWithConcurrentAppend() throws IOException {
        Assume.assumeTrue(false);  // pending change to server behavior
        String filename = directory + "/" + "Core.create0LengthFileWithConcurrentAppend.txt";
        System.out.println("Running create0LengthFileWithConcurrentAppend");

        byte [] contents = new byte[0];
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        OperationResponse resp = new OperationResponse();
        Core.concurrentAppend(filename, contents, 0, 0, true,
                client, null, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Error in ConcurrentAppend with 0-len content " + filename);
        }

        DirectoryEntry de = dir(filename);
        assertTrue("File type should be FILE for 0-len file", de.type == DirectoryEntryType.FILE);
        assertTrue("File length in DirectoryEntry should be 0 for 0-len file", de.length == 0);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file length should be 0 for 0-len file", b.length == 0);
    }


    @Test
    public void createSmallFileWithConcurrentAppend() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Core.createSmallFileWithConcurrentAppend.txt";
        System.out.println("Running createSmallFileWithConcurrentAppend");

        byte [] contents = HelperUtils.getSampleText1();
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        OperationResponse resp = new OperationResponse();
        Core.concurrentAppend(filename, contents, 0, contents.length, true,
                client, null, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Error in ConcurrentAppend with small content " + filename);
        }

        getFileContents(filename, contents.length * 2); // read, to force metadata sync

        DirectoryEntry de = dir(filename);
        assertTrue("File type should be FILE", de.type == DirectoryEntryType.FILE);
        assertTrue("File length in DirectoryEntry should match (" + de.length + "!=" + contents.length + ")",
                de.length == contents.length);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file length should match after ConcurrentAppend", b.length == contents.length);
        assertTrue("file contents should match after ConcurrentAppend", Arrays.equals(contents, b));
    }

    @Test(expected = ADLException.class)
    public void concurrentAppendWithoutAutocreate() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Core.concurrentAppendWithoutAutocreate.txt";
        System.out.println("Running concurrentAppendWithoutAutocreate");

        byte [] contents = HelperUtils.getSampleText1();
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        OperationResponse resp = new OperationResponse();
        Core.concurrentAppend(filename, contents, 0, contents.length, false,
                client, null, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "(expected) Exception from concurrentAppend " + filename);
        }
        // should throw exception
    }


    @Test
    public void concurrentAppendToExistingFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Core.concurrentAppendToExistingFile.txt";
        System.out.println("Running concurrentAppendToExistingFile");

        ByteArrayOutputStream bos = new ByteArrayOutputStream(16 * 1024);

        byte [] contents = HelperUtils.getSampleText1();
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        OperationResponse resp = new OperationResponse();
        Core.concurrentAppend(filename, contents, 0, contents.length, true,
                client, null, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Exception from concurrentAppend " + filename);
        }
        bos.write(contents);

        contents = HelperUtils.getSampleText2();
        opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        resp = new OperationResponse();
        Core.concurrentAppend(filename, contents, 0, contents.length, false,
                client, null, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Exception from concurrentAppend " + filename);
        }
        bos.write(contents);

        bos.close();
        byte[] b1 = bos.toByteArray();

        getFileContents(filename, contents.length * 2); // read, to force metadata sync

        DirectoryEntry de = dir(filename);
        assertTrue("File type should be FILE", de.type == DirectoryEntryType.FILE);
        assertTrue("File length in DirectoryEntry should match", de.length == b1.length);

        byte[] b = getFileContents(filename, b1.length * 2);
        assertTrue("file length should match after ConcurrentAppend", b.length == b1.length);
        assertTrue("file contents should match after ConcurrentAppend", Arrays.equals(b1, b));
    }



    @Test
    public void create4MBFile() throws IOException {
        Assume.assumeTrue(false);  // subsumed by TestFileSdk tests
        String filename = directory + "/" + "Core.Create4MBFile.txt";
        System.out.println("Running create4MBFile");

        byte [] contents = HelperUtils.getRandomBuffer(4 * 1024 * 1024);
        putFileContents(filename, contents, true);

        byte[] b = getFileContents(filename, contents.length * 2);
        assertTrue("file length should match what was written", b.length == contents.length);
        assertTrue("file contents should match", Arrays.equals(b, contents));
    }

    @Test
    public void create5MBFile() throws IOException {
        Assume.assumeTrue(false);  // subsumed by TestFileSdk tests
        String filename = directory + "/" + "Core.Create5MBFile.txt";
        System.out.println("Running create5MBFile");

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
        System.out.println("Running createOverwriteFile");

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
        System.out.println("Running createNoOverwriteFile");

        byte[] contents = HelperUtils.getSampleText1();
        putFileContents(filename, contents, true);

        // attempt to overwrite the same file, but with overwrite flag as false. Should fail.
        contents = HelperUtils.getSampleText2();
        putFileContents(filename, contents, false);
    }


    private void putFileContents(String filename, byte[] b, boolean overwrite) throws IOException {
        RequestOptions opts = new RequestOptions();
        OperationResponse resp = new OperationResponse();
        Core.create(filename, overwrite, null, b, 0, b.length, null, null,
            true, SyncFlag.CLOSE, client, null, opts, resp);
        if (!resp.successful) throw client.getExceptionFromResponse(resp, "Error creating file " + filename);
    }

    private byte[] getFileContents(String filename, int maxLength) throws IOException {
        byte[] b = new byte[maxLength];
        int count = 0;
        boolean eof = false;

        while (!eof && count<b.length) {
            RequestOptions opts = new RequestOptions();
            OperationResponse resp = new OperationResponse();
            InputStream in = Core.open(filename, count, 0, null, client, null, opts, resp);
            if (resp.httpResponseCode == 403 || resp.httpResponseCode == 416) {
                eof = true;
                continue;
            }
            if (!resp.successful) throw client.getExceptionFromResponse(resp, "Error reading from file " + filename);
            int bytesRead;
            while ((bytesRead = in.read(b, count, b.length - count)) != -1) {
                count += bytesRead;
                if (count >= b.length) break;
            }
            in.close();
        }
        byte[] b2 = Arrays.copyOfRange(b, 0, count);
        return b2;
    }

    private DirectoryEntry dir(String filename) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new NoRetryPolicy();
        OperationResponse resp = new OperationResponse();
        DirectoryEntry de = Core.getFileStatus(filename, UserGroupRepresentation.OID, client, null, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Error in ConcurrentAppend with null content " + filename);
        }
        return de;
    }

}
