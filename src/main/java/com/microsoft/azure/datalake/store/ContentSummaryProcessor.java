/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;


import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal class, used to do client-side enumeration of directories to produce result for
 * getContentSummary().
 *
 */
class ContentSummaryProcessor {

    private AtomicLong fileCount = new AtomicLong(0);
    private AtomicLong directoryCount = new AtomicLong(0);
    private AtomicLong totalBytes = new AtomicLong(0);
    private ProcessingQueue<DirectoryEntry> queue = new ProcessingQueue<>();
    private ADLStoreClient client;

    private static final int NUM_THREADS = 16;
    private static final int ENUMERATION_PAGESIZE = 16000;

    // this is a one-shot class, really like a functor. Do not reuse to make multiple calls.
    public ContentSummary getContentSummary(ADLStoreClient client, String dirname) throws IOException {
        this.client = client;
        DirectoryEntry de = client.getDirectoryEntry(dirname);
        if (de.type == DirectoryEntryType.FILE) {
            processFile(de);
        } else {
            queue.add(de);
            processDirectory(de);

            // Start threads in the processing thread-pool
            Thread[] threads = new Thread[NUM_THREADS];
            for (int i = 0; i < NUM_THREADS; i++) {
                threads[i] = new Thread(new ThreadProcessor());
                threads[i].start();
            }

            // wait for all threads to get done
            for (Thread t : threads) {
                try {
                    t.join();
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return new ContentSummary(totalBytes.get(), directoryCount.get(), fileCount.get(), totalBytes.get());
    }

    private class ThreadProcessor  implements Runnable {

        public void run() {
            try {
                DirectoryEntry de;
                while ((de = queue.poll()) != null) {
                    if (de.type == DirectoryEntryType.DIRECTORY) {
                        processDirectoryTree(de.fullName);
                        queue.unregister();
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
                System.exit(1000);
            }
        }
    }

    private void processDirectoryTree(String directoryName) throws IOException {
        int pagesize = ENUMERATION_PAGESIZE;
        ArrayList<DirectoryEntry> list;
        boolean eol = false;
        String startAfter = null;

        do {
            list = (ArrayList<DirectoryEntry>) enumerateDirectoryInternal(directoryName, pagesize,
                    startAfter, null, null);
            if (list == null || list.size() == 0) break;
            for (DirectoryEntry de : list) {
                if (de.type == DirectoryEntryType.DIRECTORY) {
                    queue.add(de);
                    processDirectory(de);
                }
                if (de.type == DirectoryEntryType.FILE) {
                    processFile(de);
                }
                startAfter = de.name;
            }
        } while (list.size() >= pagesize);
    }

    private void processDirectory(DirectoryEntry de) {
        directoryCount.incrementAndGet();
    }

    private void processFile(DirectoryEntry de) {
        fileCount.incrementAndGet();
        totalBytes.addAndGet(de.length);
    }

    private List<DirectoryEntry> enumerateDirectoryInternal(String path,
                                                            int maxEntriesToRetrieve,
                                                            String startAfter,
                                                            String endBefore,
                                                            UserGroupRepresentation oidOrUpn)
            throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialBackoffPolicy();
        OperationResponse resp = new OperationResponse();
        List<DirectoryEntry> dirEnt = Core.listStatus(path, startAfter, endBefore, maxEntriesToRetrieve, oidOrUpn,
                client, opts, resp);
        if (!resp.successful) {
            throw client.getExceptionFromResponse(resp, "Error enumerating directory " + path);
        }
        return dirEnt;
    }
}
