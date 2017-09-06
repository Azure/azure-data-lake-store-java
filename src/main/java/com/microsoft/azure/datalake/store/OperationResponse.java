/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import java.io.InputStream;

/**
 * information about a response from a server call.
 *
 * This class is a container for all the information from making a server call.
 *
 *
 */
public class OperationResponse {
    /**
     * whether the request was successful. Callers should always check for success before using any return value from
     * any of the calls.
     */
    public boolean successful = true;


    /**
     * The WebHDFS opCode of the remote operation
     */
    public String opCode = null;

    /**
     * the HTTP response code from the call
     */
    public int httpResponseCode;

    /**
     * the message that came with the HTTP response
     */
    public String httpResponseMessage;

    /**
     * for methods that return data from server, this field contains the
     * {@link com.microsoft.azure.datalake.store.ADLFileInputStream ADLFileInputStream}. {@code null} for methods that
     * return no data in the HTTP body.
     *
     */
    public InputStream responseStream = null;

    /**
     * the server request ID.
     */
    public String requestId = null;

    /**
     * the offset of the last committed append block in the adl stream
     */
    public long committedBlockOffset = -1;

    /**
     * the number of retries attempted before returning from the call
     */
    public int numRetries;

    /**
     * the latency of the <I>last</I> try, in milliseconds
     */
    public long lastCallLatency = 0;


    /**
     * time taken to get the token for this request, in nanoseconds. Should mostly be small.
     */
    public long tokenAcquisitionLatency = 0;

    /**
     * Content-Length of the returned HTTP body (if return was not chunked). Callers should look at both this and
     * {@link #responseChunked} values to determine whether any data was returned by server.
     */
    public long responseContentLength = 0;


    /**
     * indicates whether HTTP body used chunked for {@code Transfer-Encoding} of the response
     */
    public boolean responseChunked = false;

    /**
     * the exception name as reported by the server, if the call failed on server
     */
    public String remoteExceptionName = null;

    /**
     * the exception message as reported by the server, if the call failed on server
     */
    public String remoteExceptionMessage = null;

    /**
     * the exception's Java Class Name as reported by the server, if the call failed on server
     * This is there for WebHDFS compatibility.
     */
    public String remoteExceptionJavaClassName = null;

    /**
     * exceptions encountered when processing the request or response
     */
    public Exception ex = null;

    /**
     * error message, used for errors that originate within the SDK
     */
    public String message;

    /**
     * Comma-separated list of exceptions encountered but not thrown by this call. This may happen because of retries.
     */
    public String exceptionHistory = null;

    /**
     * Reset response object to initial state
     */
    public void reset() {
        this.successful = true;
        this.opCode = null;
        this.httpResponseCode = 0;
        this.httpResponseMessage = null;
        this.responseStream = null;
        this.requestId = null;
        this.numRetries = 0;
        this.lastCallLatency = 0;
        this.responseContentLength = 0;
        this.responseChunked = false;
        this.remoteExceptionName = null;
        this.remoteExceptionMessage = null;
        this.remoteExceptionJavaClassName = null;
        this.ex = null;
        this.message = null;
        // do not reset exceptionHistory
    }
}

