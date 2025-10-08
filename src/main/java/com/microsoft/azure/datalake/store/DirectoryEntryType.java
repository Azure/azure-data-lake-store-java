/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;


/**
 * Indicates whether a directory entry represents a file or a directory.
 */
public enum DirectoryEntryType {
    /** Entry represents a file. */
    FILE,
    /** Entry represents a directory. */
    DIRECTORY
}
