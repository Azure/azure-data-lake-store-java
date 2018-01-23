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
public class NonIdempotentRetryPolicy implements RetryPolicy {

    private int retryCount401 = 0;
    private int waitInterval = 100;

    private int retryCount429 = 0;
    private int maxRetries = 4;
    private int exponentialRetryInterval = 1000;
    private int exponentialFactor = 4;


    public boolean shouldRetry(int httpResponseCode, Exception lastException) {
        if (httpResponseCode == 401 && retryCount401 == 0) {
            // this could be because of call delay. Just retry once, in hope of token being renewed by now
            wait(waitInterval);
            retryCount401++;
            return true;
        }

        if (httpResponseCode == 429) {
            // 429 means that the backend did not change any state.
            if (retryCount429 < maxRetries) {
                wait(exponentialRetryInterval);
                exponentialRetryInterval *= exponentialFactor;
                retryCount429++;
                return true;
            } else {
                return false;  // max # of retries exhausted
            }
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
