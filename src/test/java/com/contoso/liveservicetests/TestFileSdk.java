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
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;


public class TestFileSdk {
    private final UUID instanceGuid = UUID.randomUUID();

    private static String directory = null;
    private static ADLStoreClient client = null;
    private static boolean testsEnabled = true;

    @BeforeClass
    public static void setup() throws IOException {
        Properties prop = HelperUtils.getProperties();
        AzureADToken aadToken = AzureADAuthenticator.getTokenUsingClientCreds(prop.getProperty("OAuth2TokenUrl"),
                prop.getProperty("ClientId"),
                prop.getProperty("ClientSecret") );
        UUID guid = UUID.randomUUID();
        directory = "/" + prop.getProperty("dirName") + "/" + UUID.randomUUID();
        String account = prop.getProperty("StoreAcct") + ".azuredatalakestore.net";
        client = ADLStoreClient.createClient(account, aadToken.accessToken);
        testsEnabled = Boolean.parseBoolean(prop.getProperty("SdkTestsEnabled", "true"));
    }

    @Test
    public void createDirectory() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.createDirectory/a/b/c";

        client.createDirectory(filename);
    }

    @Test
    public void createEmptyFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.createEmptyFile.txt";

        // write some text to file
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        out.close();

        // read text from file
        InputStream in = client.getReadStream(filename);
        byte[] b1 = new byte[4096]; // to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify nothing was read
        assertTrue("file length should be zero", 0 == count);
    }


    @Test
    public void smallFileNoOverwrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.smallFileNoOverwrite.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.FAIL);
        out.write(contents);
        out.close();

        // read text from file
        InputStream in = client.getReadStream(filename);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void smallFileWithUnicodeCharacters() throws IOException {
        Assume.assumeTrue(testsEnabled);

        // Contains language names in that language
        // non-AlphaNum-chars.Traditional-Chinese.Simplified-Chinese.Hebrew.Hindi.Spanish
        String unicodeFilename = "ch+ ch.官話.官话.עברית.हिंदी.español.txt";

        String testDirectory = directory + "/Sdk.smallFileWithUnicodeCharacters/";
        String filename = testDirectory + unicodeFilename;

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.FAIL);
        out.write(contents);
        out.close();

        // read text from file
        InputStream in = client.getReadStream(filename);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));

        // verify getDirectoryEntry succeeds
        client.getDirectoryEntry(filename);

        // verify it is returned correctly in enumerate of directory
        List<DirectoryEntry> list = client.enumerateDirectory(testDirectory, 10);
        for (DirectoryEntry entry : list) {
            String serverFilename = entry.name;
            String fullServerFilename = entry.fullName;
            assertTrue("file name should match", serverFilename.equals(unicodeFilename));
            assertTrue("file fullname should match", fullServerFilename.equals(filename));
            // should only be one file in there
        }
    }

    @Test(expected = IOException.class)
    public void existingFileNoOverwrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.existingFileNoOverwrite.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.FAIL);
        out.write(contents);
        out.close();

        // overwrite the text with new text - SHOULD FAIL since file already exists
        contents = HelperUtils.getSampleText2();
        out = client.createFile(filename, IfExists.FAIL);
        out.write(contents);
        out.close();  //  <<-- FAIL here
    }

    @Test
    public void smallFileWithOverwrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.smallFileWithOverwrite.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        // overwrite the text with new text
        contents = HelperUtils.getSampleText2();
        out = client.createFile(filename, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        // read text from file
        InputStream in = client.getReadStream(filename);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to the second text
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void nonExistingFileWithOverwrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.nonExistingFileWithOverwrite.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        // read text from file
        InputStream in = client.getReadStream(filename);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void large11MBWrite() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.large11MBWrite.txt";

        // write some text to file
        byte [] contents = HelperUtils.getRandomBuffer(11 * 1024 * 1024);
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        // read from file
        InputStream in = client.getReadStream(filename);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void multiple4Mbwrites() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.multiple4Mbwrites.txt";

        // do three 4mb writes
        byte [] contents = HelperUtils.getRandom4mbBuffer();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(contents.length*3);
        for (int i = 0; i<3; i++) {
            out.write(contents);
            bos.write(contents);
        }
        out.close();
        bos.close();
        byte[] b1 = bos.toByteArray();

        // read file contents
        InputStream in = client.getReadStream(filename);
        byte[] b2 = new byte[b1.length*2]; // double the size, to account for possible bloat due to bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b2, count, b2.length-count)) >=0 && count<=b2.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", b1.length == count);
        byte[] b3 = Arrays.copyOfRange(b2, 0, count);
        assertTrue("file contents should match", Arrays.equals(b1, b3));
    }

    @Test
    public void createFileAndDoManySmallWrites() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.CreateFileAndDoManySmallWrites.txt";

        // write a small text many times to file, creating a large file (multiple 4MB chunks + partial chunk)
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(13000*742);
        for (int i = 0; i<13000; i++) {  // 742 bytes * 13000 == 9.2MB upload total
            out.write(contents);
            bos.write(contents);
        }
        out.close();
        bos.close();
        byte[] b1 = bos.toByteArray();

        // read file contents
        InputStream in = client.getReadStream(filename);
        byte[] b2 = new byte[13000*742*2]; // double the size, to account for possible bloat due to bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b2, count, b2.length-count)) >=0 && count<=b2.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", b1.length == count);
        byte[] b3 = Arrays.copyOfRange(b2, 0, count);
        assertTrue("file contents should match", Arrays.equals(b1, b3));
    }

    @Test
    public void writeFilewithSmallBuffer() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.writeFilewithSmallBuffer.txt";

        // write a small text many times to file, creating a large file (multiple 4MB chunks + partial chunk)
        byte [] contents = HelperUtils.getSampleText1();
        ADLFileOutputStream adlout = client.createFile(filename, IfExists.OVERWRITE);
        OutputStream out = adlout;


        // first, write contents with default buffer size
        ByteArrayOutputStream bos = new ByteArrayOutputStream(2 * 742);
        out.write(contents);
        bos.write(contents);

        // now resize buffer and then write
        adlout.setBufferSize(37);  // set to small prime number
        out.write(contents);
        bos.write(contents);

        out.close();
        bos.close();
        byte[] b1 = bos.toByteArray();

        // read file contents
        InputStream in = client.getReadStream(filename);
        byte[] b2 = new byte[742*4]; // double the size, to account for possible bloat due to bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b2, count, b2.length-count)) >=0 && count<=b2.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", b1.length == count);
        byte[] b3 = Arrays.copyOfRange(b2, 0, count);
        assertTrue("file contents should match", Arrays.equals(b1, b3));
    }


    @Test(expected = ADLException.class)
    public void testAppendNonexistentFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.testAppendNonexistentFile.txt";

        OutputStream out = client.getAppendStream(filename);
        out.close();
    }

    @Test
    public void testAppendEmptyFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.testAppendEmptyFile.txt";

        // create empty file
        client.createEmptyFile(filename);

        // append some content into it
        OutputStream out = client.getAppendStream(filename);
        byte [] contents = HelperUtils.getSampleText1();
        out.write(contents);
        out.close();

        // read file contents
        InputStream in = client.getReadStream(filename);
        byte[] b2 = new byte[742*4]; // double the size, to account for possible bloat due to bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b2, count, b2.length-count)) >=0 && count<=b2.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b3 = Arrays.copyOfRange(b2, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b3));

    }

    @Test(expected = ADLException.class)
    public void concatZeroFiles() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.concatZeroFiles-c.txt";

        // concatenate single file
        List<String> flist = new ArrayList<String>(1);
        client.concatenateFiles(filename, flist);
    }

    @Test
    public void concatSingleFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String fn1 = directory + "/" + "Sdk.concatSingleFile.txt";
        String fn2 = directory + "/" + "Sdk.concatSingleFile-c.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(fn1, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        // concatenate single file
        List<String> flist = Arrays.asList(fn1);
        client.concatenateFiles(fn2, flist);

        // read text from file
        InputStream in = client.getReadStream(fn2);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test(expected = ADLException.class)
    public void concatSingleFileOntoItself() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.concatSingleFileOntoItself.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        // concatenate single file
        List<String> flist = Arrays.asList(filename);
        client.concatenateFiles(filename, flist);
    }


    @Test
    public void concatTwoFiles() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String fn1 = directory + "/" + "Sdk.concatTwoFiles-1.txt";
        String fn2 = directory + "/" + "Sdk.concatTwoFiles-2.txt";
        String fnc = directory + "/" + "Sdk.concatTwoFiles-c.txt";

        ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(fn1, IfExists.OVERWRITE);
        out.write(contents);
        bos.write(contents);
        out.close();

        // write text to another file
        contents = HelperUtils.getSampleText2();
        out = client.createFile(fn2, IfExists.OVERWRITE);
        out.write(contents);
        bos.write(contents);
        out.close();

        bos.close();
        contents = bos.toByteArray();

        // concatenate files
        List<String> flist = Arrays.asList(fn1, fn2);
        client.concatenateFiles(fnc, flist);

        // read text from file
        InputStream in = client.getReadStream(fnc);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void concatWithSourceFileRepeated() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String fn1 = directory + "/" + "Sdk.concatWithSourceFileRepeated-1.txt";
        String fn2 = directory + "/" + "Sdk.concatWithSourceFileRepeated-2.txt";
        String fnc = directory + "/" + "Sdk.concatWithSourceFileRepeated-c.txt";

        ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(fn1, IfExists.OVERWRITE);
        out.write(contents);
        bos.write(contents);
        out.close();

        // write text to another file
        contents = HelperUtils.getSampleText2();
        out = client.createFile(fn2, IfExists.OVERWRITE);
        out.write(contents);
        bos.write(contents);
        out.close();

        bos.close();
        contents = bos.toByteArray();

        // concatenate files with fn1 repeated - should fail
        List<String> flist = Arrays.asList(fn1, fn2, fn1);
        try {
            client.concatenateFiles(fnc, flist);
            fail("Concat should fail if a file is used repeatedly in sources");
        } catch (ADLException ex) {
            // expected
        }
    }



    @Test
    public void concatThreeFiles() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String fn1 = directory + "/" + "Sdk.concatThreeFiles-1.txt";
        String fn2 = directory + "/" + "Sdk.concatThreeFiles-2.txt";
        String fn3 = directory + "/" + "Sdk.concatThreeFiles-3.txt";
        String fnc = directory + "/" + "Sdk.concatThreeFiles-c.txt";

        ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(fn1, IfExists.OVERWRITE);
        out.write(contents);
        bos.write(contents);
        out.close();

        contents = HelperUtils.getSampleText2();
        out = client.createFile(fn2, IfExists.OVERWRITE);
        out.write(contents);
        bos.write(contents);
        out.close();

        contents = HelperUtils.getRandomBuffer(1024);
        out = client.createFile(fn3, IfExists.OVERWRITE);
        out.write(contents);
        bos.write(contents);
        out.close();

        bos.close();
        contents = bos.toByteArray();

        // concatenate files
        List<String> flist = Arrays.asList(fn1, fn2, fn3);
        client.concatenateFiles(fnc, flist);

        // read text from file
        InputStream in = client.getReadStream(fnc);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void renameFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.renameFile.txt";
        String fnr = directory + "/" + "Sdk.renameFile-r.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        //rename file
        boolean succeeded = client.rename(filename, fnr);
        assertTrue("rename should not return false", succeeded);

        // read text from file
        InputStream in = client.getReadStream(fnr);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));
    }

    @Test
    public void renameNonExistentFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.renameNonExistentFile.txt";
        String fnr = directory + "/" + "Sdk.renameNonExistentFile-r.txt";

        boolean succeeded = client.rename(filename, fnr);
        assertFalse("rename of non-existent file should return false", succeeded);
    }

    @Test
    public void renameOntoSelf() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.renameOntoSelf.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        boolean succeeded = client.rename(filename, filename);
        assertFalse("rename should not return true", succeeded);
    }

    @Test
    public void deleteNonExistentFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.deleteNonExistentFile.txt";

        boolean succeeded = client.delete(filename);
        assertFalse("delete() should not return true on a non-existent file", succeeded);
    }

    @Test
    public void deleteFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.deleteFile.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        boolean succeeded = client.delete(filename);
        assertTrue("delete() should not return false on a file delete", succeeded);
    }

    @Test
    public void getDirectoryEntryforFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "Sdk.getDirectoryEntryforFile.txt";

        DirectoryEntry d;

        try {
            d = client.getDirectoryEntry(filename);
            fail("getDirectoryEnrty on non-existent file should throw exception");
        } catch (ADLException ex) {
            assertTrue("Exception should be 404", ex.httpResponseCode == 404);
        }

        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        client.getDirectoryEntry(filename); // should not throw exception - file should exist on server now
        out.write(contents);
        out.close();

        d = client.getDirectoryEntry(filename);
        assertTrue("File fullname should match", d.fullName.equals(filename));
        assertTrue("File name should match", d.name.equals(filename.substring(filename.lastIndexOf('/')+1)));
        assertTrue("File should be of type FILE", d.type == DirectoryEntryType.FILE);
        assertTrue("File length should match", d.length == contents.length);
        assertTrue("user should not be missing", d.user!=null && !d.user.trim().equals(""));
        assertTrue("group should not be missing", d.group!=null && !d.group.trim().equals(""));
        assertTrue("blocksize should always be 256MB from server", d.blocksize == 256*1024*1024);
        assertTrue("replicartion factor size should always be 1 from server", d.replicationFactor == 1);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR_OF_DAY, -1);
        Date dt = cal.getTime();
        assertTrue("mtime should be recent", d.lastAccessTime.after(dt));
        assertTrue("atime should be recent", d.lastModifiedTime.after(dt));

        Pattern rwxPattern = Pattern.compile("[0-7r-][0-7w-][0-7x-]");
        assertTrue("permission should match rwx or Octal pattern", rwxPattern.matcher(d.permission).matches());
    }

    @Test
    public void getDirectoryEntryforDirectory() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String dirname = directory + "/" + "getDirectoryEntryforDirectory/a/b/c";

        boolean succeeded = client.createDirectory(dirname);
        assertTrue("Directory creation shoul dnot fail", succeeded);

        DirectoryEntry de = client.getDirectoryEntry(dirname);
        assertTrue("Directory fullname should match", de.fullName.equals(dirname));
        assertTrue("Directory name should match", de.name.equals(dirname.substring(dirname.lastIndexOf('/')+1)));
        assertTrue("Directory should be of type DIRECTORY", de.type == DirectoryEntryType.DIRECTORY);
        assertTrue("Directory length should be zero", de.length == 0);
        assertTrue("user should not be missing", de.user!=null && !de.user.trim().equals(""));
        assertTrue("group should not be missing", de.group!=null && !de.group.trim().equals(""));
        assertTrue("blocksize should always be 0 for directory. Got " + de.blocksize, de.blocksize == 0);
        assertTrue("replication factor size should always be 0 for directory. Got " + de.replicationFactor, de.replicationFactor == 0);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR_OF_DAY, -1);
        Date dt = cal.getTime();
        assertTrue("mtime should be recent", de.lastAccessTime.after(dt));
        assertTrue("atime should be recent", de.lastModifiedTime.after(dt));

        Pattern rwxPattern = Pattern.compile("[0-7r-][0-7w-][0-7x-]");
        assertTrue("permission should match rwx or Octal pattern", rwxPattern.matcher(de.permission).matches());
    }

    @Test
    public void deleteDirectoryRecursive() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String dirname = directory + "/" + "deleteDirectoryRecursive";

        String fn1 = dirname + "/a/b/c/f1.txt";
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(fn1, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        String fn2 = dirname + "/a/b/f2.txt";
        contents = HelperUtils.getSampleText2();
        out = client.createFile(fn2, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        String parentDir = dirname + "/a";
        boolean succeeded = client.deleteRecursive(parentDir);
        assertTrue("recursive delete should return true", succeeded);

        try {
            client.getDirectoryEntry(parentDir);
            fail("getDirectoryEntry should fail on a deleted directory");
        } catch (ADLException ex) {
            if (ex.httpResponseCode!=404) throw ex;
        }
    }

    @Test(expected = IOException.class)
    public void deleteDirNonRecursiveNonEmpty() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String dirname = directory + "/" + "deleteDirNonRecursiveNonEmpty";

        String fn1 = dirname + "/a/b/c/f1.txt";
        byte[] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(fn1, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        String fn2 = dirname + "/a/b/f2.txt";
        contents = HelperUtils.getSampleText2();
        out = client.createFile(fn2, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        String parentDir = dirname + "/a";
        client.delete(parentDir);
    }
    

    @Test(expected = ADLException.class)
    public void deleteDirectoryNonRecursive() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String dirname = directory + "/" + "deleteDirectoryNonRecursive";

        String fn1 = dirname + "/a/b/c/f1.txt";
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(fn1, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        String fn2 = dirname + "/a/b/f2.txt";
        contents = HelperUtils.getSampleText2();
        out = client.createFile(fn2, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        String parentDir = dirname + "/a";
        client.delete(parentDir);
    }

    @Test
    public void contentSummaryForFile() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String filename = directory + "/" + "contentSummaryForFile.txt";

        // write some text to file
        byte [] contents = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(filename, IfExists.OVERWRITE);
        out.write(contents);
        out.close();

        ContentSummary contentSummary = client.getContentSummary(filename);
        assertTrue("file length in content summary should match", contentSummary.length == contents.length);
        assertTrue("directoryCount for file should be zero", contentSummary.directoryCount == 0);
        assertTrue("fileCount for file should be 1", contentSummary.fileCount == 1);
        assertTrue("spaceConsumed in content summary should match", contentSummary.spaceConsumed == contents.length);
    }

    @Test
    public void contentSummaryForDirectory() throws IOException {
        Assume.assumeTrue(testsEnabled);
        String dirname = directory + "/" + "contentSummaryForDirectory";
        String fn1 = dirname + "/" + "a.txt";
        String fn2 = dirname + "/" + "b.txt";
        String dn2 = dirname + "/" + "foo";

        // create a subdirectory
        client.createDirectory(dn2);

        // write some text to two files
        byte [] contents1 = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(fn1, IfExists.OVERWRITE);
        out.write(contents1);
        out.close();

        byte [] contents2 = HelperUtils.getSampleText2();
        out = client.createFile(fn2, IfExists.OVERWRITE);
        out.write(contents2);
        out.close();

        long totalLength = contents1.length + contents2.length;

        ContentSummary contentSummary = client.getContentSummary(dirname);
        assertTrue("length in content summary for dir should match sum of all files in dir", contentSummary.length == totalLength);
        assertTrue("directoryCount for this directory should be two", contentSummary.directoryCount == 2);
        assertTrue("fileCount for this directory should be two", contentSummary.fileCount == 2);
        assertTrue("spaceConsumed in content summary should match", contentSummary.spaceConsumed == totalLength);
    }

    @Test
    public void pathPrefix() throws IOException, URISyntaxException {
        Assume.assumeTrue(testsEnabled);
        String prefix = directory + "/" + "pathPrefix";
        client.setOptions(new ADLStoreOptions().setFilePathPrefix(prefix));

        // Do a series of operations on the prefix-enabled client and
        // ensure they succeed

        // create directory
        String dirname = "/foo/bar";
        client.createDirectory(dirname);

        ByteArrayOutputStream bos = new ByteArrayOutputStream(4096);

        // write some text to two files
        String fn1 = dirname + "/" + "a.txt";
        byte [] contents1 = HelperUtils.getSampleText1();
        OutputStream out = client.createFile(fn1, IfExists.OVERWRITE);
        out.write(contents1);
        bos.write(contents1);
        out.close();

        String fn2 = dirname + "/" + "b.txt";
        byte [] contents2 = HelperUtils.getSampleText2();
        out = client.createFile(fn2, IfExists.OVERWRITE);
        out.write(contents2);
        bos.write(contents2);
        out.close();

        bos.close();
        byte[] contents = bos.toByteArray();

        // concatenate the files
        String fn3 = dirname + "/" + "c.txt";
        List<String> flist = Arrays.asList(fn1, fn2);
        client.concatenateFiles(fn3, flist);

        // rename the concatenated file
        String fn4 = dirname + "/" + "d.txt";
        client.rename(fn3, fn4);

        // read text from file
        InputStream in = client.getReadStream(fn4);
        byte[] b1 = new byte[contents.length*2]; // double the size, to account for bloat due to possible bug in upload
        int bytesRead;
        int count = 0;
        while ((bytesRead = in.read(b1, count, b1.length-count)) >=0 && count<=b1.length ) {
            count += bytesRead;
        }

        // verify what was read is identical to what was written
        assertTrue("file length should match what was written", contents.length == count);
        byte[] b2 = Arrays.copyOfRange(b1, 0, count);
        assertTrue("file contents should match", Arrays.equals(contents, b2));

        // delete file
        client.delete(fn4);
    }
}
