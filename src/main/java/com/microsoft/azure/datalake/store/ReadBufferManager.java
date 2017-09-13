package com.microsoft.azure.datalake.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Internal-use only; do not use.
 *
 * Manage the read-ahead buffers and queues.
 *
 * The class has a pool of 16 byte-buffers it manages ('buffers' variable). It has a freeList, which contains the
 * index in the buffers array of buffers that are available to take. It also contains the in-progress and completed
 * lists.
 *
 * When a read-ahead comes in, the manager looks to see if there is an available buffer in the free-list. If there is,
 * it creates a ReadBuffer object and assigns the byte-buffer to it. The ReadBuffer object tracks a read-ahead through
 * it's life-cycle. At the end if the life-cycle, the buffer is returned back (by adding it's index to free-list), and
 * the ReadBuffer object is garbage collected.
 *
 * The ReadBuffer is initially put in the readAheadQueue (i.e., the waiting queue). When a worker thread picks it up,
 * the ReadBuffer is moved to the inProgressList. When the worker threads gets done with the read, the ReadBuffer is
 * moved to the completedReadList. the buffer sits there until the space is needed by another read-ahead - it is only
 * evicted if the space is needed. Upon eviction, the byte[] buffer is returned to the free pool and the ReadBuffer
 * object is GC'ed. If a read request comes in for an offset that is still in the readAheadQueue (i.e., nothing has
 * been done on it), then the object is removed from the queue, and the call returns 0, indicating to the calling thread
 * to do the read itself (since there is no sense in waiting in the queue for some unbounded future time).
 *
 * Overhead of the ReadBufferManager: there are a total of 16 buffers (so 16 * 4MB = 64MB total memory overhead). Also,
 * there are 8 worker threads (that are either reading data so blocked waiting for IO, or are blocked on readAheadQueue,
 * waiting for read-ahead requests to arrive).
 *
 * Side benefit: since the prefetched buffers sit around in the completedReadList until evicted, there may be a slight
 * benefit of cache hits, although the main benefit is expected to be from the read-ahead, not so much from the cache
 * effect.
 */
class ReadBufferManager {
    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.ReadBufferManager");

    private static final int numBuffers = 16;
    private final static int blocksize = 4 * 1024 * 1024;
    private final static int numThreads  = 8;
    private final static int thresholdAgeMilliseconds = 3000; // have to see if 3 seconds is a good threshold

    private Thread[] threads = new Thread[numThreads];
    private byte[][] buffers;    // array of byte[] buffers, to hold the data that is read
    private Stack<Integer> freeList = new Stack<Integer>();   // indices in buffers[] array that are available


    private Queue<ReadBuffer> readAheadQueue = new LinkedList<ReadBuffer>(); // queue of requests that are not picked up by any worker thread yet
    private LinkedList<ReadBuffer> inProgressList = new LinkedList<ReadBuffer>(); // requests being processed by worker threads
    private LinkedList<ReadBuffer> completedReadList = new LinkedList<ReadBuffer>(); // buffers available for reading
    private static final ReadBufferManager bufferManager; // singleton, initialized in static initialization block


    static {
        bufferManager = new ReadBufferManager();
        bufferManager.init();
    }

    static ReadBufferManager getBufferManager() { return bufferManager; }

    private void init() {
        buffers = new byte[numBuffers][];
        for (int i = 0; i < numBuffers; i++) {
            buffers[i] = new byte[blocksize];  // same buffers are reused. The byte array never goes back to GC
            freeList.add(i);
        }
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread(new ReadBufferWorker(i));
            t.setDaemon(true);
            threads[i] = t;
            t.setName("ADLS-prefetch-" + i);
            t.start();
        }
        ReadBufferWorker.unleashWorkers.countDown();
    }

    // hide instance constructor
    private ReadBufferManager() {
    }


    /*
    *
    *  ADLFileInputStream-facing methods
    *
    */


    /**
     * {@link ADLFileInputStream} calls this method to queue read-aheads
     * @param file The {@link ADLFileInputStream} for which to do the read-ahead
     * @param requestedOffset The offset in the file which shoukd be read
     * @param requestedLength The length to read
     */
    void queueReadAhead(ADLFileInputStream file, long requestedOffset, int requestedLength) {
        if (log.isTraceEnabled())
            log.trace("Start Queueing readAhead for " + file.getFilename() + " offset " + requestedOffset + " length " + requestedLength);
        ReadBuffer buffer;
        synchronized (this) {
            if (isAlreadyQueued(file, requestedOffset)) return; // already queued, do not queue again
            if (freeList.size() == 0 && !tryEvict()) return; // no buffers available, cannot queue anything

            buffer = new ReadBuffer();
            buffer.file = file;
            buffer.offset = requestedOffset;
            buffer.length = 0;
            buffer.requestedLength = requestedLength;
            buffer.status = ReadBufferStatus.NOT_AVAILABLE;
            buffer.latch = new CountDownLatch(1);
            Integer bufferIndex = freeList.pop();  // will return a value, since we have checked size > 0 already
            buffer.buffer = buffers[bufferIndex];
            buffer.bufferindex = bufferIndex;
            readAheadQueue.add(buffer);
            notifyAll();
        }
        if (log.isTraceEnabled()) {
            log.trace("Done q-ing readAhead for file " + file.getFilename() + " offset " + requestedOffset + " buffer idx " + buffer.bufferindex);
        }
    }


    /**
     * {@link ADLFileInputStream} calls this method read any bytes already available in a buffer (thereby saving a
     * remote read). This returns the bytes if the data already exists in buffer. If there is a buffer that is reading
     * the requested offset, then this method blocks until that read completes. If the data is queued in a read-ahead
     * but not picked up by a worker thread yet, then it cancels that read-ahead and reports cache miss. This is because
     * depending on worker thread availability, the read-ahead may take a while - the calling thread can do it's own
     * read to get the data faster (copmared to the read waiting in queue for an indeterminate amount of time).
     *
     * @param file the file to read bytes for
     * @param position the offset in the file to do a read for
     * @param length the length to read
     * @param buffer the buffer to read data into. Note that the buffer will be written into from offset 0.
     * @return the number of bytes read
     */
    int getBlock(ADLFileInputStream file, long position, int length, byte[] buffer) {
        // not synchronized, so have to be careful with locking
        if (log.isTraceEnabled())
            log.trace("getBlock for file " + file.getFilename() + " position " + position + " thread " + Thread.currentThread().getName());

        { // block scope, to scope the usage of readbuf. The two synchronized blocks should not share any data, to
          // ensure there are no race conditions.
            ReadBuffer readBuf;
            synchronized (this) {
                clearFromReadAheadQueue(file, position);
                readBuf = getFromList(inProgressList, file, position);
            }
            if (readBuf != null) {         // if in in-progress queue, then block for it
                try {
                    if (log.isTraceEnabled())
                        log.trace("got a relevant read buffer for file " + file.getFilename() + " offset " + readBuf.offset + " buffer idx " + readBuf.bufferindex);
                    readBuf.latch.await();  // blocking wait on the caller stream's thread
                    // Note on correctness: readBuf gets out of inProgressList only in 1 place: after worker thread
                    // is done processing it (in doneReading). There, the latch is set after removing the buffer from
                    // inProgressList. So this latch is safe to be outside the synchronized block.
                    // Putting it in synchronized would result in a deadlock, since this thread would be holding the lock
                    // while waiting, so no one will be able to  change any state. If this becomes more complex in the future,
                    // then the latch cane be removed and replaced with wait/notify whenever inProgressList is touched.
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
                if (log.isTraceEnabled())
                    log.trace("latch done for file " + file.getFilename() + " buffer idx " + readBuf.bufferindex + " length " + readBuf.length);
            }
        }

        int bytesRead = 0;
        synchronized (this) {
            bytesRead = getBlockFromCompletedQueue(file, position, length, buffer);
        }
        if (bytesRead > 0) {
            if (log.isTraceEnabled())
                log.trace("Done read from Cache for " + file.getFilename() + " position " + position + " length " + bytesRead);
            return bytesRead;
        }

        // otherwise, just say we got nothing - calling thread can do it's own read
        return 0;
    }

    /*
    *
    *  Internal methods
    *
    */

    /**
     * If any buffer in the completedlist can be reclaimed then reclaim it and return the buffer to free list.
     * The objective is to find just one buffer - there is no advantage to evicting more than one.
     * @return whether the eviction succeeeded - i.e., were we able to free up one buffer
     */
    private synchronized boolean tryEvict() {
        ReadBuffer nodeToEvict = null;
        if (completedReadList.size() <= 0) return false;  // there are no evict-able buffers

        // first, try buffers where all bytes have been consumed (approximated as first and last bytes consumed)
        for (ReadBuffer buf : completedReadList) {
            if (buf.firstByteConsumed && buf.lastByteConsumed) {
                nodeToEvict = buf;
                break;
            }
        }
        if (nodeToEvict != null) return evict(nodeToEvict);

        // next, try buffers where any bytes have been consumed (may be a bad idea? have to experiment and see)
        for (ReadBuffer buf : completedReadList) {
            if (buf.anyByteConsumed) {
                nodeToEvict = buf;
                break;
            }
        }
        if (nodeToEvict != null) return evict(nodeToEvict);

        // next, try any old nodes that have not been consumed
        long earliestBirthday = Long.MAX_VALUE;
        for (ReadBuffer buf : completedReadList) {
            if (buf.birthday < earliestBirthday) {
                nodeToEvict = buf;
                earliestBirthday = buf.birthday;
            }
        }
        if ((System.currentTimeMillis() - earliestBirthday > thresholdAgeMilliseconds) && (nodeToEvict != null) )
        { return evict(nodeToEvict); }

        // nothing can be evicted
        return false;
    }

    private boolean evict(ReadBuffer buf) {
        freeList.push(buf.bufferindex);
        completedReadList.remove(buf);
        if (log.isTraceEnabled())
            log.trace("Evicting buffer idx " + buf.bufferindex + "; was used for file " + buf.file.getFilename() +
                    " offset " + buf.offset + " length " + buf.length);

        return true;
    }


    private boolean isAlreadyQueued(ADLFileInputStream file, long requestedOffset) {
        // returns true if any part of the buffer is already queued
        return (isInList(readAheadQueue, file, requestedOffset) ||
                isInList(inProgressList, file, requestedOffset) ||
                isInList(completedReadList, file, requestedOffset) );
    }

    private boolean isInList(Collection<ReadBuffer> list, ADLFileInputStream file, long requestedOffset ) {
        return (getFromList(list, file, requestedOffset) != null);
    }

    private ReadBuffer getFromList(Collection<ReadBuffer> list, ADLFileInputStream file, long requestedOffset ) {
        for (ReadBuffer buffer : list) {
            if (buffer.file == file) {
                if (buffer.status == ReadBufferStatus.AVAILABLE
                        && requestedOffset >= buffer.offset
                        && requestedOffset < buffer.offset + buffer.length
                        ) {
                    return buffer;
                } else if (requestedOffset >= buffer.offset
                        && requestedOffset < buffer.offset + buffer.requestedLength
                        ) {
                    return buffer;
                }
            }
        }
        return null;
    }

    private void clearFromReadAheadQueue(ADLFileInputStream file, long requestedOffset) {
        ReadBuffer buffer = getFromList(readAheadQueue, file, requestedOffset);
        if (buffer != null) {
            readAheadQueue.remove(buffer);
            notifyAll();   // lock is held in calling method
            freeList.push(buffer.bufferindex);
        }
    }

    private int getBlockFromCompletedQueue(ADLFileInputStream file, long position, int length, byte[] buffer) {
        ReadBuffer buf = getFromList(completedReadList, file, position);
        if (buf == null || position >= buf.offset + buf.length) return 0;
        int cursor = (int) (position - buf.offset);
        int availableLengthInBuffer = buf.length - cursor;
        int lengthToCopy = Math.min(length, availableLengthInBuffer);
        System.arraycopy(buf.buffer, cursor, buffer, 0, lengthToCopy);
        if (cursor == 0) buf.firstByteConsumed = true;
        if (cursor + lengthToCopy == buf.length) buf.lastByteConsumed = true;
        buf.anyByteConsumed = true;
        return lengthToCopy;
    }

    /*
    *
    *  ReadBufferWorker-thread-facing methods
    *
    */

    /**
     * ReadBufferWorker thread calls this to get the next buffer that it should work on.
     * @return {@link ReadBuffer}
     * @throws InterruptedException if thread is interrupted
     */
    ReadBuffer getNextBlockToRead() throws InterruptedException {
        ReadBuffer buffer = null;
        synchronized (this) {
            //buffer = readAheadQueue.take();  // blocking method
            while (readAheadQueue.size() == 0) wait();
            buffer = readAheadQueue.remove();
            notifyAll();
            if (buffer == null) return null;            // should never happen
            buffer.status = ReadBufferStatus.READING_IN_PROGRESS;
            inProgressList.add(buffer);
        }
        if (log.isTraceEnabled())
            log.trace("ReadBufferWorker picked file " + buffer.file.getFilename() + " for offset " + buffer.offset);
        return buffer;
    }

    /**
     *
     * ReadBufferWorker thread calls this method to post completion
     *
     * @param buffer the buffer whose read was completed
     * @param result the {@link ReadBufferStatus} after the read operation in the worker thread
     * @param bytesActuallyRead the number of bytes that the worker thread was actually able to read
     */
    void doneReading(ReadBuffer buffer, ReadBufferStatus result, int bytesActuallyRead) {
        if (log.isTraceEnabled())
            log.trace("ReadBufferWorker completed file " + buffer.file.getFilename() + " for offset " + buffer.offset + " bytes " + bytesActuallyRead);
        synchronized (this) {
            inProgressList.remove(buffer);
            if (result == ReadBufferStatus.AVAILABLE && bytesActuallyRead > 0) {
                buffer.status = ReadBufferStatus.AVAILABLE;
                buffer.birthday = System.currentTimeMillis();
                buffer.length = bytesActuallyRead;
                completedReadList.add(buffer);
            } else {
                freeList.push(buffer.bufferindex);
                // buffer should go out of scope after the end of the calling method in ReadBufferWorker, and eligible for GC
            }
        }
        //outside the synchronized, since anyone receiving a wake-up from the latch must see safe-published results
        buffer.latch.countDown(); // wake up waiting threads (if any)
    }
}
