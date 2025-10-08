/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.acl;


/**
 * Entity type targeted by an ACL entry (user, group, other, or mask).
 */
public enum AclType {
    /** ACL entry applies to a specific user. */
    USER,
    /** ACL entry applies to members of a specific group. */
    GROUP,
    /** ACL entry applies to all users not otherwise matched. */
    OTHER,
    /** ACL entry defines the mask (effective permissions) for named entries. */
    MASK
}
