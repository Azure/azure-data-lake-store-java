/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.retrypolicies;

/**
 * the RetryPolicy controls whether a failed request should be retried, and how long to wait before retrying.
 *  <P>
 * Implementations of this interface implement different retry policies.
 * </P>
 */
public interface RetryPolicy {
    /**
     * boolean indicating whether a failed request should be retried. Implementations can use the
     * HTTP response code and any exceptions from the last failure to decide whether to retry.
     * <P>
     * If the retry policy requires a wait before the next try, then the {@code shouldRetry} method should wait for
     * the appropriate time before responding back. i.e., there is not an explicit contract for waits, but it
     * is implicit in the time taken by the {@code shouldRetry} method to return.
     * </P>
     * @param httpResponseCode the HTTP response code received
     * @param lastException any exceptions encountered while processing the last request
     * @return boolean indicating whether the request should be retried
     */
    boolean shouldRetry(int httpResponseCode, Exception lastException);
}
