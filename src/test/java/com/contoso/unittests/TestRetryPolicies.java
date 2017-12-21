/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.contoso.unittests;

import com.contoso.helpers.HelperUtils;
import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

public class TestRetryPolicies {

    private boolean testsEnabled = true;
    private boolean longRunningEnabled = true;

    private class Timer
    {
        private long startTime = System.nanoTime();

        int getElapsedMilliseconds()
        {
            return (int)((System.nanoTime() - startTime) / 1000000);
        }
    }


    @Before
    public void setup() throws IOException {
        Properties prop = HelperUtils.getProperties();

        testsEnabled = Boolean.parseBoolean(prop.getProperty("MockTestsEnabled", "true"));
        longRunningEnabled = Boolean.parseBoolean(prop.getProperty("LongRunningTestsEnabled", "true"));
    }

    @Test
    public void testExponentialRetryRunOutOfRetries() {
        Assume.assumeTrue(testsEnabled);
        Assume.assumeTrue(longRunningEnabled);
        System.out.println("Running testExponentialRetryRunOutOfRetries");


        ExponentialBackoffPolicy retryPolicy = new ExponentialBackoffPolicy();

        {
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(1000, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == true);
        }

        {
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(4000, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == true);
        }

        {
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(16000, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == true);
        }

        {
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(64000, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == true);
        }

        {
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(0, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == false);
        }
    }

    @Test
    public void testExponentialRetryDoesNotWaitUnnecessarily() {
        Assume.assumeTrue(testsEnabled);
        Assume.assumeTrue(longRunningEnabled);
        System.out.println("Running testExponentialRetryDoesNotWaitUnnecessarily");

        ExponentialBackoffPolicy retryPolicy = new ExponentialBackoffPolicy();

        {
            wait(1000);
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(0, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == true);
        }

        {
            wait(4000);
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(0, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == true);
        }

        {
            wait(16000);
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(0, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == true);
        }

        {
            wait(64000);
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(0, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == true);
        }

        {
            Timer timer = new Timer();
            boolean shouldRetry = retryPolicy.shouldRetry(503, null);
            assertEquals(0, timer.getElapsedMilliseconds(), 500);
            assertTrue("shouldRetry was incorrect", shouldRetry == false);
        }
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