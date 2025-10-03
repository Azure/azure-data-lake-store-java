/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.acl;


import java.util.List;

/**
 * Result containing the ACL and permission details for a given file or directory as returned by
 * {@link com.microsoft.azure.datalake.store.ADLStoreClient#getAclStatus(String)}.
 */
public class AclStatus {
    /**
     * Creates an empty status container whose fields can be populated by a service response.
     */
    public AclStatus() {
    }

    /**
     * {@code List<AclEntry>} containing the list of Acl entries for a file
     */
    public List<AclEntry> aclSpec;

    /**
     * String containing the ID of the owner of the file
     */
    public String owner;

    /**
     * String containing the ID of the group that owns this file
     */
    public String group;

    /**
     * Unix permissions for the file/directory in Octal form
     */
    public String octalPermissions;

    /**
     * Sticky bit (only meaningful for a directory)
     */
    public boolean stickyBit;

}
