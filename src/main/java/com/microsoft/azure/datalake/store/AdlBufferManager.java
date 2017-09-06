/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * {@code AdlBufferManager} is used to get and return large buffers
 *
 */
public class AdlBufferManager {

    private class Buffer {
        public Buffer(byte[] storage) {
            this.storage = storage;
            this.lastAccessed = System.currentTimeMillis();
        }

        public byte[] storage;
        public long lastAccessed;
    }

    private int bufferSize;
    private ConcurrentLinkedDeque<Buffer> buffers = new ConcurrentLinkedDeque<>();

    public AdlBufferManager(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public byte[] getBuffer() {
        Buffer last = buffers.pollLast();
        if (last == null) {
            // Allocate a new buffer and return it
            return new byte[bufferSize];
        }

        Buffer first = buffers.getFirst();
        while (first != null && first.lastAccessed + 60000 < System.currentTimeMillis()) {
            // Remove old buffers
            first = buffers.pollFirst();
        }

        return last.storage;
    }

    public void releaseBuffer(byte[] buffer) {
        if (buffer.length == bufferSize) {
            buffers.push(new Buffer(buffer));
        }
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}