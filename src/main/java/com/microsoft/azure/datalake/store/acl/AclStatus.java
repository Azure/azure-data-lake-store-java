/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.acl;


import java.util.List;

/**
 * Object returned by the {@link com.microsoft.azure.datalake.store.ADLStoreClient#getAclStatus(String)}  getAclStatus}
 * call, that contains the Acl and Permission information for that file or directory.
 *
 */
public class AclStatus {
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
