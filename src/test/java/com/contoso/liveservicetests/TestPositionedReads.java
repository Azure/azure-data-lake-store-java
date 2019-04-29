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
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class TestPositionedReads {
    private final UUID instanceGuid = UUID.randomUUID();

    private static String directory = null;
    private static ADLStoreClient client = null;
    private static boolean testsEnabled = true;

    @BeforeClass
    public static void setup() throws IOException {
        Properties prop;
        AzureADToken aadToken;

        prop = HelperUtils.getProperties();
        aadToken = AzureADAuthenticator.getTokenUsingClientCreds(prop.getProperty("OAuth2TokenUrl"),
        prop.getProperty("ClientId"),
        prop.getProperty("ClientSecret") );
        UUID guid = UUID.randomUUID();
        directory = "/" + prop.getProperty("dirName", "unitTests") + "/" + UUID.randomUUID();
        String account = prop.getProperty("StoreAcct") + ".azuredatalakestore.net";
        client = ADLStoreClient.createClient(account, aadToken);
        testsEnabled = Boolean.parseBoolean(prop.getProperty("PositionedReadsTestsEnabled", "true"));
    }

    @AfterClass
    public static void teardown() throws IOException {
        client.deleteRecursive(directory);
    }

    @Test
    public void smallFileSeek() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "PositionedReads.smallFileSeek.dat";
        System.out.println("Running smallFileSeek");

        int fileLength = 1024;
        OutputStream stream = client.createFile(filename, IfExists.OVERWRITE);
        byte[] content = HelperUtils.getRandomBuffer(fileLength);
        stream.write(content);
        stream.close();

        ADLFileInputStream instream = client.getReadStream(filename);
        assertTrue("File length should be as expected", instream.length() == fileLength);
        assertTrue("File position initially should be 0", instream.getPos() == 0);
        instream.seek(instream.length() - 2);
        assertTrue("Premature EOF at (-2)", instream.read() != -1);
        assertTrue("Premature EOF at (-1)", instream.read() != -1);
        assertTrue("read() should return -1 at EOF", instream.read() == -1);
    }

    @Test
    public void seekAndCheck() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "PositionedReads.seekAndCheck.txt";
        System.out.println("Running seekAndCheck");

        OutputStream stream = client.createFile(filename, IfExists.OVERWRITE);
        byte[] content = HelperUtils.getSampleText1();
        stream.write(content);
        stream.close();

        ADLFileInputStream in = client.getReadStream(filename);
        in.setBufferSize(20);
        assertTrue("should be able to seek past buffer in the beginning",checkByteAt(21, in, content));
        assertTrue("should be able to seek to beginning from anywhere",checkByteAt(0,  in,  content));
        assertTrue("should be able to seek past buffer",checkByteAt(60, in, content));
        assertTrue("re-seeking to same location should work",checkByteAt(60, in, content));
        assertTrue("seeking within buffer should work",checkByteAt(61, in, content));
        assertTrue("seeking to within buffer should work",checkByteAt(75, in, content));
        assertTrue("back-and-forth within buffer should work",checkByteAt(74, in, content));
        assertTrue("more back-and-forth within buffer should work",checkByteAt(62, in, content));
        assertTrue("seeking backwards should work",checkByteAt(21, in, content));
        assertTrue("seeking forwards should work",checkByteAt(45, in, content));
        assertTrue("seeking forward with many buffer's gap should work",checkByteAt(80, in, content));
        assertTrue("seeking backwards with many buffer's gap should work",checkByteAt(23, in, content));
        assertTrue("seeking backwards to one byte before buffer should work",checkByteAt(22, in, content));
        assertTrue("more seeks - just for good measure",checkByteAt(99, in, content));
        assertTrue("even more seeks - just for good measure",checkByteAt(11, in, content));
        assertTrue("seeks to early offset",checkByteAt(3, in, content));
        assertTrue("seek back to zero after many seeks",checkByteAt(0, in, content));
    }

    private static boolean checkByteAt(long pos, ADLFileInputStream in, byte[] content) throws IOException {
        in.seek(pos);
        int expected = content[(int)pos];
        int actual = in.read();
        if (actual != expected) {
            System.out.format("Mismatch at %d: expected %c, found %c\n", pos, expected, actual );
            return false;
        } else {
            return true;
        }
    }

    @Test
    public void resizeBuffer() throws IOException {
        Assume.assumeTrue(false);
        String filename = directory + "/" + "PositionedReads.resizeBuffer.txt";

        OutputStream stream = client.createFile(filename, IfExists.OVERWRITE);
        byte[] content = HelperUtils.getSampleText1();
        stream.write(content);
        stream.close();

        ADLFileInputStream instr = client.getReadStream(filename);
        byte[] b1 = new byte[200];
        instr.read(b1);

        instr.setBufferSize(87);
        instr.read(b1);



        assertTrue("This test needs to be finished", false);
    }
}
