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
    private long lastAttemptStartTime = System.nanoTime();

    /**
     * Creates a policy using the default retry counts and intervals.
     */
    public ExponentialBackoffPolicy() {
    }

    /**
     * Creates a policy configured with a custom retry count and base backoff interval.
     *
     * @param maxRetries maximum number of retries
     * @param exponentialRetryInterval (starting) interval to use for exponential backoff retries (in milliseconds)
     */
    public ExponentialBackoffPolicy(int maxRetries, int exponentialRetryInterval) {
        this(maxRetries, 0, exponentialRetryInterval);
    }

    /**
     * Creates a policy configured with a custom retry count and interval while retaining a now-ignored linear retry parameter.
     *
     * @param maxRetries maximum number of retries.
     * @param linearRetryInterval retained for backwards compatibility and ignored.
     * @param exponentialRetryInterval (starting) interval to use for exponential backoff retries (in milliseconds).
     */
    public ExponentialBackoffPolicy(int maxRetries, int linearRetryInterval, int exponentialRetryInterval) {
        if (linearRetryInterval < 0) {
            throw new IllegalArgumentException("linearRetryInterval must be non-negative.");
        }
        this.maxRetries = maxRetries;
        this.exponentialRetryInterval = exponentialRetryInterval;
    }

    /**
     * Creates a policy with custom retry count, interval, and multiplicative factor applied after each attempt.
     *
     * @param maxRetries maximum number of retries.
     * @param linearRetryInterval retained for backwards compatibility and ignored.
     * @param exponentialRetryInterval (starting) interval to use for exponential backoff retries (in milliseconds).
     * @param exponentialFactor multiplier applied to the retry interval after each retry.
     */
    public ExponentialBackoffPolicy(int maxRetries, int linearRetryInterval, int exponentialRetryInterval, int exponentialFactor) {
        this(maxRetries, linearRetryInterval, exponentialRetryInterval);
        this.exponentialFactor = exponentialFactor;
    }

    /**
     * Determines whether another retry should be attempted for the supplied HTTP response code and exception.
     *
     * @param httpResponseCode HTTP status code received from the service.
     * @param lastException exception thrown during the last attempt, if any.
     * @return {@code true} when a retry should be attempted; otherwise {@code false}.
     */
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
                int timeSpent = (int)((System.nanoTime() - lastAttemptStartTime) / 1000000);
                wait(exponentialRetryInterval - timeSpent);
                exponentialRetryInterval *= exponentialFactor;
                retryCount++;
                lastAttemptStartTime = System.nanoTime();
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
        if (milliseconds <= 0) {
            return;
        }

        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();   // http://www.ibm.com/developerworks/library/j-jtp05236/
        }
    }
}
