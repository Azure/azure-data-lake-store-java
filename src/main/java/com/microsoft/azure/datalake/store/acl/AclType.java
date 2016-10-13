/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.acl;


/**
 * Type of Acl entry (user, group, other, or mask).
 */
public enum AclType {
    USER,
    GROUP,
    OTHER,
    MASK
}
