/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.acl;


/**
 * Scope of an ACL entry, indicating whether permissions apply to the resource itself or to newly created children.
 */
public enum AclScope {
    /** Applies directly to the target resource. */
    ACCESS,
    /** Applies to child resources created under the target directory. */
    DEFAULT
}
