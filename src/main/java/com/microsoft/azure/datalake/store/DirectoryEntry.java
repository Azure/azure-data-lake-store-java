/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import java.util.Date;

/**
 * filesystem metadata of a directory enrty (a file or a directory) in ADL.
 */
public class DirectoryEntry {

    /**
     * the filename (minus the path) of the direcotry entry
     */
    public final String name;

    /**
     * the full path of the directory enrty.
     */
    public final String fullName;

    /**
     * the length of a file. zero for directories.
     */
    public final long length;

    /**
     * the ID of the group that owns this file/directory.
     */
    public final String group;

    /**
     * the ID of the user that owns this file/directory.
     */
    public final String user;

    /**
     * the timestamp of the last time the file was accessed
     */
    public final Date lastAccessTime;

    /**
     * the timestamp of the last time the file was modified
     */
    public final Date lastModifiedTime;

    /**
     * {@link DirectoryEntryType} enum indicating whether the object is a file or a directory
     */
    public final DirectoryEntryType type;

    /**
     * Block size reported by server.
     * This is present for compatibility with WebHDFS - in the case of Azure Data Lake store this is always 256MB.
     *
     */
    public final long blocksize;

    /**
     * Replication Factor reported by server.
     * This is present for compatibility with WebHDFS - in the case of Azure Data Lake store this is always
     * reported as 1 - the Azure Data Lake store does appropriate replication on the server side to ensure
     * durability.
     *
     */
    public final int replicationFactor;

    /**
     * boolean indicating whether file has ACLs set on it.
     */
    public final boolean aclBit;

    /**
     * Date+time at which the file expires, as UTC time. It is null if the file has no expiry. It is null for
     * directories.
     */
    public final Date expiryTime;


    /**
     * the unix-style permission string for this file or directory
     */
    public final String permission;

    public DirectoryEntry(String name,
                   String fullName,
                   long length,
                   String group,
                   String user,
                   Date lastAccessTime,
                   Date lastModifiedTime,
                   DirectoryEntryType type,
                   long blocksize,
                   int replicationFactor,
                   String permission,
                   boolean aclBit,
                   Date expiryTime) {
        this.name = name;
        this.fullName = fullName;
        this.length = length;
        this.group = group;
        this.user = user;
        this.lastAccessTime = new Date(lastAccessTime.getTime()); // creating a new instance, since the passed-in Date is mutable
        this.lastModifiedTime = new Date(lastModifiedTime.getTime()); // basically this is copying the date from passed in date
        this.type = type;
        this.permission = permission;
        this.blocksize = blocksize;
        this.replicationFactor = replicationFactor;
        this.aclBit = aclBit;
        this.expiryTime = expiryTime;
    }
}

