/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;


import java.io.IOException;

/**
 * Exception type returned by Azure Data Lake SDK methods. Derives from {@link IOException}.
 * Contains a number of additional fields that contain information about
 * the success or failure of a server call.
 */
public class ADLException extends IOException {

    /**
     * the HTTP response code returned by the server
     */
    public int httpResponseCode;

    /**
     * The HTTP response message
     */
    public String httpResponseMessage;

    /**
     * The Server request ID
     */
    public String requestId = null;

    /**
     * the number of retries attempted before the call failed
     */
    public int numRetries;

    /**
     * the latency of the call. If retries were made, then this contains the latency of the
     * last retry.
     */
    public long lastCallLatency = 0;

    /**
     * The content length of the response, if the response contained one.
     * It can be zero if the response conatined no body, or if the body was sent using
     * chunked transfer encoding.
     *
     */
    public long responseContentLength = 0;

    /**
     * The remote exception name returned by the server in an HTTP error message.
     */
    public String remoteExceptionName = null;

    /**
     * The remote exception message returned by the server in an HTTP error message.
     */
    public String remoteExceptionMessage = null;

    /**
     * The remote exception's java class name returned by the server in an HTTP error message.
     */
    public String remoteExceptionJavaClassName = null;

    public ADLException(String message) {
        super(message);
    }

    public ADLException(String message, Throwable initCause) {
        super(message, initCause);
    }
}
