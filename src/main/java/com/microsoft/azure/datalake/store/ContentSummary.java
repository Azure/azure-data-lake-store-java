/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

/**
 * structure that contains the return values from getContentSummary call.
 */
public class ContentSummary {

    /**
     * length of file
     */
    public final long length;

    /**
     * number of subdirectories under a directory
     */
    public final long directoryCount;

    /**
     * number of files under a directory
     */
    public final long fileCount;

    /**
     * total space consumed by a directory
     */
    public final long spaceConsumed;

    /**
     * Creates a new summary populated with the statistics returned from the service.
     *
     * @param length length of the file in bytes.
     * @param directoryCount number of child directories.
     * @param fileCount number of child files.
     * @param spaceConsumed space consumed by the directory hierarchy in bytes.
     */
    public ContentSummary(
        long length,
        long directoryCount,
        long fileCount,
        long spaceConsumed
    ) {
        this.length = length;
        this.directoryCount = directoryCount;
        this.fileCount = fileCount;
        this.spaceConsumed = spaceConsumed;
    }
}
