/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.retrypolicies.RetryPolicy;


/**
 * common options to control the behavior of server calls
 */
public class RequestOptions {
    /**
     * the timeout (in milliseconds) to use for the request. This is used for both
     * the readTimeout and the connectTimeout for the request, so
     * in effect the actual timout is two times the specified timeout.
     * Default is 60,000 (60 seconds).
     */
    public int timeout = 60000;

    /**
     * the client request ID. the SDK generates a UUID if a request ID is not specified.
     */
    public String requestid = null;

    /**
     * the {@link RetryPolicy} to use for the request
     */
    public RetryPolicy retryPolicy = null;
}
