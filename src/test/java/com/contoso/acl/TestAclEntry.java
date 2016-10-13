/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.contoso.acl;

import com.contoso.helpers.HelperUtils;
import com.microsoft.azure.datalake.store.acl.AclEntry;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

public class TestAclEntry {
    final UUID instanceGuid = UUID.randomUUID();
    static Properties prop = null;
    static boolean testsEnabled = true;

    @BeforeClass
    public static void setup() throws IOException {
        prop = HelperUtils.getProperties();
        testsEnabled = Boolean.parseBoolean(prop.getProperty("acl.AclEntryTestsEnabled", "true"));
    }

    @Test
    public void parseTests() {
        compareToCanonical("user:hello:rwx", "user:hello:rwx");
        compareToCanonical("user::rwx   ", "user::rwx");
        compareToCanonical("group:AA1-hdhg-hngDjdfh-23928:rwx", "group:AA1-hdhg-hngDjdfh-23928:rwx");
        compareToCanonical("group::rwx   ", "group::rwx");
        compareToCanonical("mask::   RwX", "mask::rwx");

        compareToCanonical("default:user:hello:rwx", "default:user:hello:rwx");
        compareToCanonical("default:user ::---   ", "default:user::---");
        compareToCanonical("default: group: AA1-hdhg-hngDjdfh-23928:rwx", "default:group:AA1-hdhg-hngDjdfh-23928:rwx");
        compareToCanonical("default:group  ::   R-X", "default:group::r-x");
        compareToCanonical("default:mask::   RwX", "default:mask::rwx");
    }

    private void compareToCanonical(String input, String expectedCanonical) {
        String actualCanonical = AclEntry.parseAclEntry(input).toString();
        assertTrue("failed " + input + "/" + expectedCanonical + "/" + actualCanonical,
                   actualCanonical.equals(expectedCanonical));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAclEntry1() {
        AclEntry.parseAclEntry("user:hello", false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAclEntry2() {
        AclEntry.parseAclEntry("user:hello:rwx:h");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAclEntry3() {
        AclEntry.parseAclEntry("user:hello:rwwx");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAclEntry4() {
        AclEntry.parseAclEntry("default:mask:hello:rwx");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAclEntry5() {
        AclEntry.parseAclEntry("default::hello:rwx");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAclEntry6() {
        AclEntry.parseAclEntry(":user:hello:rwx");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAclEntry7() {
        AclEntry.parseAclEntry("other:hello:rwx");
    }
}
