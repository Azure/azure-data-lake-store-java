/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.contoso.helpers;


import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.util.Properties;

public class HelperUtils {

    private static Properties prop = null;
    public static Properties getProperties() throws IOException {
        if (prop==null) {
            Properties defaultProps = new Properties();
            defaultProps.load(new FileInputStream("target/config.properties"));
            prop = new Properties(defaultProps);
            prop.load(new FileInputStream("target/creds.properties"));
        }
        return prop;
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


}
