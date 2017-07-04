/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import java.util.concurrent.ArrayBlockingQueue;


/**
 * {@code LatencyTracker} keeps track of client-preceived request latencies, to be reported on the next REST request.
 * Every request adds its result (success/failure and latency) to LatencyTracker. When a request is made,
 * the SDK checks LatencyTracker to see if there are any latency stats to be reported. If so, the stats are added
 * as an HTTP header ({@code x-ms-adl-client-latency}) on the next request.
 * <P>
 * To disable this reporting, user can call {@link #disable()}.
 * </P>
 * Contents of data reported back:
 * <UL>
 * <LI>Client Request ID of last request</LI>
 * <LI>latency in milliseconds</LI>
 * <LI>error code (if request failed)</LI>
 * <LI>Operation</LI>
 * <LI>Request+response body Size</LI>
 * <LI>number for AzureDataLakeStoreClient that made this call</LI>
 * </UL>
 *
 */
public class LatencyTracker {
    /*
     Schema:
     Single entry, comma separated:
      1. Client Request ID
      2. latency in milliseconds
      3. error code (if request failed)
      4. Operation
      5. Request+response body Size (if available, zero otherwise)
      6. Instance of ADLStoreClient (a unique number per instance in this VM)

     Multiple entries can be on a single request. Entries will be separated by semicolons
     Limit max entries on a single request to three, to limit increase in HTTP request size.
    */

    private static final ArrayBlockingQueue<String> Q = new ArrayBlockingQueue<String>(256);
    private static final int MAXPERLINE = 3;
    private static volatile boolean disabled = false;

    private LatencyTracker() {} // Prevent instantiation - static methods only

    /**
     * Disable reporting of client-perceived latency stats to the server.
     * <P>
     * This is a static method that disables all future reporting from this JVM instance.
     * </P>
     *
     */
    public static synchronized void disable() {
        // using synchronized causes update to disabled to be published, so other threads will see updated value.
        // Deadlocks:
        //    Since this is the only method that acquires lock on the class object, deadlock with Q's lock is not an
        //        issue (i.e., lock order is same throughout the class: LatencyTracker.class, then Q)
        disabled = true;
        Q.clear();
        // The clear does not guarantee that Q will be empty afterwards - e.g., if another thread was in the middle
        // of add. However, the clear is not critical. Also, disable is one-way, so a little bit of crud leftover
        // doesnt matter. If in the future we offer re-enable, then the enable would have to clear the Q, to prevent
        // very old entries from being sent.
    }

    static void addLatency(String clientRequestId, int retryNum, long latency, String operation, long size, long clientId) {
        if (disabled) return;
        String line = String.format("%s.%d,%d,,%s,%d,%d", clientRequestId, retryNum, latency, operation, size, clientId);
        Q.offer(line); // non-blocking append. If queue is full then silently discard
    }

    static void addError(String clientRequestId, int retryNum, long latency, String error, String operation, long size, long clientId) {
        if (disabled) return;
        String line = String.format("%s.%d,%d,%s,%s,%d,%d", clientRequestId, retryNum, latency, error, operation, size, clientId);
        Q.offer(line); // non-blocking append. If queue is full then silently discard
    }

    static String get() {
        if (disabled) return null;
        int count = 0;
        String entry = null;
        String separator = "";
        StringBuilder line = new StringBuilder(MAXPERLINE * 2);

        do {
            entry = Q.poll();
            if (entry != null)
             {
                line.append(separator);
                line.append(entry);
                separator = ";";
                count++;
            }
        } while (entry != null && count <= MAXPERLINE);
        return (count > 0) ? line.toString() : null;
    }
}
