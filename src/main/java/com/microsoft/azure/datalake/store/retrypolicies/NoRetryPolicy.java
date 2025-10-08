/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.retrypolicies;

/**
 * No retry ever. Always returns false, indicating that erequest should not be retried.
 *
 * This should be used when retrying is not safe, and user wants at-most-once semantics with the call. This is
 * useful for non-idempotent methods, where the error returned by the last call does not conclusively indicate
 * success or failure of the call. For example, if an append times out but succeeds on the back-end , then
 * retrying it may append the data twice to the file.
 *
 */
public class NoRetryPolicy implements RetryPolicy {

    private int retryCount = 0;
    private int waitInterval = 100;


    /**
     * Creates a retry policy that retries at most once for an initial 401 response.
     */
    public NoRetryPolicy() {
    }

    /**
     * Determines whether another retry should be attempted for the supplied HTTP response code and exception.
     *
     * @param httpResponseCode HTTP status code received from the service.
     * @param lastException exception thrown during the last attempt, if any.
     * @return {@code true} when a retry should be attempted; otherwise {@code false}.
     */
    public boolean shouldRetry(int httpResponseCode, Exception lastException) {
        if (httpResponseCode == 401 && retryCount == 0) {
            // to mitigate a special problem with intermittent 401's on calls
            wait(waitInterval);
            retryCount++;
            return true;
        }
        return false;
    }

    private void wait(int milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();   // http://www.ibm.com/developerworks/library/j-jtp05236/
        }
    }

}
