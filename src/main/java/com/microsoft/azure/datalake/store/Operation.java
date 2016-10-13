/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;


/**
 * The WebHDFS methods, and their associated properties (e.g., what HTTP method to use, etc.)
 */
enum Operation {
    OPEN               ("OPEN",               "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    GETFILESTATUS      ("GETFILESTATUS",      "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    MSGETFILESTATUS    ("MSGETFILESTATUS",    "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    LISTSTATUS         ("LISTSTATUS",         "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    MSLISTSTATUS       ("MSLISTSTATUS",       "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    GETCONTENTSUMMARY  ("GETCONTENTSUMMARY",  "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    GETFILECHECKSUM    ("GETFILECHECKSUM",    "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    GETACLSTATUS       ("GETACLSTATUS",       "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    MSGETACLSTATUS     ("MSGETACLSTATUS",     "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    CHECKACCESS        ("CHECKACCESS",        "GET",    C.requiresBodyFalse, C.returnsBodyFalse,  C.isExtFalse),
    CREATE             ("CREATE",             "PUT",    C.requiresBodyTrue,  C.returnsBodyFalse, C.isExtFalse),
    MKDIRS             ("MKDIRS",             "PUT",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    RENAME             ("RENAME",             "PUT",    C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    SETOWNER           ("SETOWNER",           "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    SETPERMISSION      ("SETPERMISSION",      "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    SETTIMES           ("SETTIMES",           "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    MODIFYACLENTRIES   ("MODIFYACLENTRIES",   "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    REMOVEACLENTRIES   ("REMOVEACLENTRIES",   "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    REMOVEDEFAULTACL   ("REMOVEDEFAULTACL",   "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    REMOVEACL          ("REMOVEACL",          "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    SETACL             ("SETACL",             "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    CREATENONRECURSIVE ("CREATENONRECURSIVE", "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    APPEND             ("APPEND",             "POST",   C.requiresBodyTrue,  C.returnsBodyFalse, C.isExtFalse),
    CONCAT             ("CONCAT",             "POST",   C.requiresBodyFalse, C.returnsBodyFalse, C.isExtFalse),
    MSCONCAT           ("MSCONCAT",           "POST",   C.requiresBodyTrue,  C.returnsBodyFalse, C.isExtFalse),
    DELETE             ("DELETE",             "DELETE", C.requiresBodyFalse, C.returnsBodyTrue,  C.isExtFalse),
    CONCURRENTAPPEND   ("CONCURRENTAPPEND",   "POST",   C.requiresBodyTrue,  C.returnsBodyFalse,  C.isExtTrue),
    SETEXPIRY          ("SETEXPIRY",          "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtTrue),
    GETFILEINFO        ("GETFILEINFO",        "GET",    C.requiresBodyFalse, C.returnsBodyFalse, C.isExtTrue);

    String name;
    String method;
    boolean requiresBody;
    boolean returnsBody;
    boolean isExt;


    Operation(String name, String method, boolean requiresBody, boolean returnsBody, boolean isExt) {
        this.name = name;
        this.method = method;
        this.requiresBody = requiresBody;
        this.returnsBody = returnsBody;
        this.isExt = isExt;
    }

    private static class C {
        static final boolean requiresBodyTrue = true;
        static final boolean requiresBodyFalse = false;
        static final boolean returnsBodyTrue = true;
        static final boolean returnsBodyFalse = false;
        static final boolean isExtTrue = true;
        static final boolean isExtFalse = false;
    }
}

