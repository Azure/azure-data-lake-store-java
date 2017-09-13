package com.microsoft.azure.datalake.store;


import com.microsoft.azure.datalake.store.ReadBuffer;
import com.microsoft.azure.datalake.store.ReadBufferManager;
import com.microsoft.azure.datalake.store.ReadBufferStatus;

import java.util.concurrent.CountDownLatch;

/**
 * Internal use only - do not use.
 * The method running in the worker threads.
 *
 *
 */
class ReadBufferWorker implements Runnable {

    static final CountDownLatch unleashWorkers = new CountDownLatch(1);
    private int id;

    ReadBufferWorker(int id) {
        this.id = id;
    }

    /**
     * Waits until a buffer becomes available in ReadAheadQueue.
     * Once a buffer becomes available, reads the file specified in it and then posts results back to buffer manager.
     * Rinse and repeat. Forever.
     */
    public void run() {
        try {
            unleashWorkers.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        ReadBufferManager bufferManager = ReadBufferManager.getBufferManager();
        ReadBuffer buffer;
        while (true) {
            try {
                buffer = bufferManager.getNextBlockToRead();   // blocks, until a buffer is available for this thread
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
            if (buffer != null) {
                try {
                    // do the actual read, from the file.
                    int bytesRead = buffer.file.readRemote(buffer.offset, buffer.buffer, 0, buffer.requestedLength, true);
                    bufferManager.doneReading(buffer, ReadBufferStatus.AVAILABLE, bytesRead);  // post result back to ReadBufferManager
                } catch (Exception ex) {
                    bufferManager.doneReading(buffer, ReadBufferStatus.READ_FAILED, 0);
                }
            }
        }
    }
}
