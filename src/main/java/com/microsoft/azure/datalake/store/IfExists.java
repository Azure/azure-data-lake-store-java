/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

/**
 * Enum specifying actions to take if attempting to create a file that already exists.
 */
public enum IfExists {
    /**
     * Overwrite the file
     */
    OVERWRITE,

    /**
     * Fail the request
     */
    FAIL
}
