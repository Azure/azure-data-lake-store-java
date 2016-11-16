/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store.retrypolicies;

/**
 * implements different retry decisions based on the error.
 *
 * <UL>
 *     <LI>For nonretryable errors (3xx, most 4xx, and some 5xx return codes), do no retry.</LI>
 *     <LI>For throttling error, do a retry with exponential backoff</LI>
 *     <LI>for all other errors, do a retry with linear backoff</LI>
 * </UL>
 */
public class ExponentialBackoffPolicy implements RetryPolicy {

    private int retryCount = 0;
    private int maxRetries = 4;
    private int exponentialRetryInterval = 1000;
    private int exponentialFactor = 4;

    public ExponentialBackoffPolicy() {
    }

    /**
     * @param maxRetries maximum number of retries
     * @param linearRetryInterval interval to use for linear retries (in milliseconds)
     * @param exponentialRetryInterval (starting) interval to use for exponential backoff retries (in milliseconds)
     */
    public ExponentialBackoffPolicy(int maxRetries, int linearRetryInterval, int exponentialRetryInterval) {
        this.maxRetries = maxRetries;
        this.exponentialRetryInterval = exponentialRetryInterval;
    }

    public boolean shouldRetry(int httpResponseCode, Exception lastException) {

        // Non-retryable error
        if (      (httpResponseCode >= 300 && httpResponseCode < 500   // 3xx and 4xx, except specific ones below
                                           && httpResponseCode != 408
                                           && httpResponseCode != 429
                                           && httpResponseCode != 401)
               || (httpResponseCode == 501) // Not Implemented
               || (httpResponseCode == 505) // Version Not Supported
               ) {
            return false;
        }

        // Retryable error, retry with exponential backoff
        if ( lastException!=null || httpResponseCode >=500    // exception or 5xx, + specific ones below
                                 || httpResponseCode == 408
                                 || httpResponseCode == 429
                                 || httpResponseCode == 401) {
            if (retryCount < maxRetries) {
                wait(exponentialRetryInterval);
                exponentialRetryInterval *= exponentialFactor;
                retryCount++;
                return true;
            } else {
                return false;  // max # of retries exhausted
            }
        }

        // these are not errors - this method should never have been called with this
        if (httpResponseCode >= 100 && httpResponseCode <300)
        {
            return false;
        }

        // Dont know what happened - we should never get here
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
