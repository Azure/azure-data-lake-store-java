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
    OPEN               ("OPEN",               "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    GETFILESTATUS      ("GETFILESTATUS",      "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    MSGETFILESTATUS    ("MSGETFILESTATUS",    "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    LISTSTATUS         ("LISTSTATUS",         "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    MSLISTSTATUS       ("MSLISTSTATUS",       "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    GETCONTENTSUMMARY  ("GETCONTENTSUMMARY",  "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    GETFILECHECKSUM    ("GETFILECHECKSUM",    "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    GETACLSTATUS       ("GETACLSTATUS",       "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    MSGETACLSTATUS     ("MSGETACLSTATUS",     "GET",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    CHECKACCESS        ("CHECKACCESS",        "GET",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    CREATE             ("CREATE",             "PUT",    C.requiresBodyTrue,  C.returnsBodyFalse, C.webHdfs),
    MKDIRS             ("MKDIRS",             "PUT",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    RENAME             ("RENAME",             "PUT",    C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    SETOWNER           ("SETOWNER",           "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    SETPERMISSION      ("SETPERMISSION",      "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    SETTIMES           ("SETTIMES",           "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    MODIFYACLENTRIES   ("MODIFYACLENTRIES",   "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    REMOVEACLENTRIES   ("REMOVEACLENTRIES",   "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    REMOVEDEFAULTACL   ("REMOVEDEFAULTACL",   "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    REMOVEACL          ("REMOVEACL",          "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    SETACL             ("SETACL",             "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    CREATENONRECURSIVE ("CREATENONRECURSIVE", "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    APPEND             ("APPEND",             "POST",   C.requiresBodyTrue,  C.returnsBodyFalse, C.webHdfs),
    CONCAT             ("CONCAT",             "POST",   C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfs),
    MSCONCAT           ("MSCONCAT",           "POST",   C.requiresBodyTrue,  C.returnsBodyFalse, C.webHdfs),
    DELETE             ("DELETE",             "DELETE", C.requiresBodyFalse, C.returnsBodyTrue,  C.webHdfs),
    CONCURRENTAPPEND   ("CONCURRENTAPPEND",   "POST",   C.requiresBodyTrue,  C.returnsBodyFalse, C.webHdfsExt),
    SETEXPIRY          ("SETEXPIRY",          "PUT",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfsExt),
    GETFILEINFO        ("GETFILEINFO",        "GET",    C.requiresBodyFalse, C.returnsBodyFalse, C.webHdfsExt);

    String name;
    String method;
    boolean requiresBody;
    boolean returnsBody;
    String namespace;

    Operation(String name, String method, boolean requiresBody, boolean returnsBody, String namespace) {
        this.name = name;
        this.method = method;
        this.requiresBody = requiresBody;
        this.returnsBody = returnsBody;
        this.namespace = namespace;
    }

    private static class C {
        static final boolean requiresBodyTrue = true;
        static final boolean requiresBodyFalse = false;
        static final boolean returnsBodyTrue = true;
        static final boolean returnsBodyFalse = false;
        static final String webHdfs = "/webhdfs/v1";
        static final String webHdfsExt = "/WebHdfsExt";
    }
}

