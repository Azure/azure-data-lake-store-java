/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.contoso.acl;

import com.contoso.helpers.HelperUtils;

import com.microsoft.azure.datalake.store.acl.AclAction;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;


public class TestAclAction {
    final UUID instanceGuid = UUID.randomUUID();
    static Properties prop = null;
    static boolean testsEnabled = true;

    @BeforeClass
    public static void setup() throws IOException {
        prop = HelperUtils.getProperties();
        testsEnabled = Boolean.parseBoolean(prop.getProperty("acl.AclActionTestsEnabled", "true"));
    }

    @Test
    public void testAclActionRwx() {
        assertTrue(AclAction.fromRwx("---") == AclAction.NONE);
        assertTrue(AclAction.fromRwx("--x") == AclAction.EXECUTE);
        assertTrue(AclAction.fromRwx("-w-") == AclAction.WRITE);
        assertTrue(AclAction.fromRwx("-wx") == AclAction.WRITE_EXECUTE);
        assertTrue(AclAction.fromRwx("r--") == AclAction.READ);
        assertTrue(AclAction.fromRwx("r-x") == AclAction.READ_EXECUTE);
        assertTrue(AclAction.fromRwx("rw-") == AclAction.READ_WRITE);
        assertTrue(AclAction.fromRwx("rwx") == AclAction.ALL);

        assertTrue(AclAction.fromRwx("R-x") == AclAction.READ_EXECUTE);
        assertTrue(AclAction.fromRwx("r-x  ") == AclAction.READ_EXECUTE);
        assertTrue(AclAction.fromRwx("  RWx") == AclAction.ALL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAclActionRwxMalformed1() {
        AclAction.fromRwx("rws");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAclActionRwxMalformed2() {
        AclAction.fromRwx("r wx");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAclActionRwxMalformed3() {
        AclAction.fromRwx("rrwx");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAclActionRwxMalformed4() {
        AclAction.fromRwx("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAclActionRwxMalformed5() {
        AclAction.fromRwx(null);
    }

    @Test
    public void testAclActionOctal() {
        assertTrue(AclAction.fromOctal(0) == AclAction.NONE);
        assertTrue(AclAction.fromOctal(1) == AclAction.EXECUTE);
        assertTrue(AclAction.fromOctal(2) == AclAction.WRITE);
        assertTrue(AclAction.fromOctal(3) == AclAction.WRITE_EXECUTE);
        assertTrue(AclAction.fromOctal(4) == AclAction.READ);
        assertTrue(AclAction.fromOctal(5) == AclAction.READ_EXECUTE);
        assertTrue(AclAction.fromOctal(6) == AclAction.READ_WRITE);
        assertTrue(AclAction.fromOctal(7) == AclAction.ALL);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAclActionOctalMalformed1() {
        AclAction.fromOctal(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAclActionOctalMalformed2() {
        AclAction.fromOctal(8);
    }
    
    @Test
    public void testSerialization() {
        assertTrue(AclAction.NONE.toString().equals("---"));
        assertTrue(AclAction.EXECUTE.toString().equals("--x"));
        assertTrue(AclAction.WRITE.toString().equals("-w-"));
        assertTrue(AclAction.WRITE_EXECUTE.toString().equals("-wx"));
        assertTrue(AclAction.READ.toString().equals("r--"));
        assertTrue(AclAction.READ_EXECUTE.toString().equals("r-x"));
        assertTrue(AclAction.READ_WRITE.toString().equals("rw-"));
        assertTrue(AclAction.ALL.toString().equals("rwx"));
    }
}
