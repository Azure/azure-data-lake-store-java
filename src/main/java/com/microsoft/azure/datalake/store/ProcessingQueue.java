/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;


import java.util.LinkedList;
import java.util.Queue;

     /*
     ProcessingQueue keeps track of directories queued to process.

     Desired behavior:
       1. Caller can enqueue directories to process that will be picked up by an open thread (add() method)
       2. Caller can dequeue directories when it is ready to process (poll() method)
       3. If the queue is empty, then caller blocks until an item becomes available in the queue (behavior of poll())
     These three properties above can provide required behavior during runtime. However, we need something
     more to detect completion - queue being empty is not good enough, since queue can also be empty
     during run when all directories queued have been picked up by some thread or another, and they may queue
     more as they find out about more items. So the termination condition is really that "queue is empty *and*
     no threads are processing any items". So we have to also keep track of items being processed. So additional
     requirements:
       4. Caller should indicate when it is done processing an item it popped from the queue (unregister() method)
       5. Caller should indicate when it has started processing an item. (that is implicit here, since
          dequeueing an item automatically implies start of processing)
       6. Caller processing the last item initiates completion, as per termination condition above (done in unregister())
       7. poll() can return null if processing is complete, but not otherwise


     Notes:
     The processing threads spend their time doing I/O most of the time. If there isnt enough work, they
     spend time blocked on the semaphore in poll().

     At the end, all but one threads block on poll(), only the last thread out detects completion - i.e., the thread
     that was processing the last item. It is this thread's job to now wake up all the other threads from their
     blocking state so they can complete.
     */


/**
 * Internal class, used to coordinate among the multiple threads doing recursive directory traversal
 * for getContentSummary
 *
 * @param <T> The type of items in the queue
 */
class ProcessingQueue<T> {
    private Queue<T> internalQueue = new LinkedList<>();
    private int processorCount = 0;

    public synchronized void add(T item) {
        if (item == null) throw new IllegalArgumentException("Cannot put null into queue");
        internalQueue.add(item);
        this.notifyAll();
    }

    public synchronized T poll() {
        try {
            while (isQueueEmpty() && !done())
                this.wait();
            if (!isQueueEmpty()) {
                processorCount++;  // current thread is now processing the item we pop
                return internalQueue.poll();
            }
            if (done()) {
                return null;
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        return null; // just to keep the compiler happy - it couldn't infer that all code-paths are covered above.
    }

    public synchronized void unregister() {
        processorCount--;
        if (processorCount < 0) {
            throw new IllegalStateException("too many unregister()'s. processorCount is now " + processorCount);
        }
        if (done()) this.notifyAll();
    }

    private boolean done() {
        return (processorCount == 0 && isQueueEmpty());
    }

    private boolean isQueueEmpty() {
        return (internalQueue.peek() == null);
    }
}
