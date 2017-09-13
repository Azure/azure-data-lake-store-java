package com.microsoft.azure.datalake.store;


import java.util.concurrent.CountDownLatch;

/**
 * This object represents the buffer state as it is going through it's lifecycle.
 * The buffer (the byte array) itself is assigned to this object from a free pool,
 * so we do not create tons of objects in large object heap.
 */
class ReadBuffer {
    ADLFileInputStream file;
    long offset;                   // offset within the file for the buffer
    int length;                    // actual length, set after the buffer is filles
    int requestedLength;           // requested length of the read
    byte[] buffer;                 // the buffer itself
    int bufferindex = -1;          // index in the buffers array in Buffer manager
    ReadBufferStatus status;             // status of the buffer
    CountDownLatch latch = null;   // signaled when the buffer is done reading, so any client
                                              // waiting on this buffer gets unblocked

    // fields to help with eviction logic
    long birthday = 0;  // tick at which buffer became available to read
    boolean firstByteConsumed = false;
    boolean lastByteConsumed = false;
    boolean anyByteConsumed = false;
}
