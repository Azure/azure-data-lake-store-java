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

public class TestAclSpec {
    final UUID instanceGuid = UUID.randomUUID();
    static Properties prop = null;
    static boolean testsEnabled = true;

    @BeforeClass
    public static void setup() throws IOException {
        prop = HelperUtils.getProperties();
        testsEnabled = Boolean.parseBoolean(prop.getProperty("acl.AclSpecTestsEnabled", "true"));
    }

    private void compareToCanonical(String input, String expectedCanonical) {
        String actualCanonical = AclEntry.aclListToString(AclEntry.parseAclSpec(input));  //.toString();
        assertTrue("failed " + input + "/" + expectedCanonical + "/" + actualCanonical,
                actualCanonical.equals(expectedCanonical));
    }

    @Test
    public void parseTests() {
        compareToCanonical("user:hello:rwx", "user:hello:rwx");
        compareToCanonical("user::rwx   ,default:mask::r-- ,mask: :-wx", "user::rwx,default:mask::r--,mask::-wx");
        compareToCanonical("group:a:rwx, user:b:r--, default:other::--x ", "group:a:rwx,user:b:r--,default:other::--x");

        compareToCanonical("user:bob:rwx,", "user:bob:rwx");
        compareToCanonical(",user:bob:rwx", "user:bob:rwx");
        compareToCanonical("user:bob:rwx,,  ,,group:sales:r--", "user:bob:rwx,group:sales:r--");
        compareToCanonical("  ", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidAclSpec1() {
        AclEntry.parseAclSpec("user:bob:rwx,user:hello");
    }
}
