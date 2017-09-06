/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.UUID;

/**
 * {@code ADLFileOutputStream} is used to add data to an Azure Data Lake File.
 * It is a buffering stream that accumulates user writes, and then writes to the server
 * in chunks. Default chunk size is 4MB.
 *
 * <P><B>Thread-safety</B>: Note that methods in this class are NOT thread-safe.</P>
 *
 */
public class ADLFileOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.ADLFileOutputStream");

    private final String filename;
    private final ADLStoreClient client;
    private final boolean isCreate;
    private final String leaseId;

    private int blocksize = 4 * 1024 *1024;  // default buffer size of 4MB
    private byte[] buffer = null;            // will be initialized at first use

    private int cursor = 0;
    private long remoteCursor = 0;
    private boolean streamClosed = false;
    private boolean lastFlushUpdatedMetadata = false;

    HttpContext httpContext;
    AdlBufferManager bufferManager = new AdlBufferManager(blocksize);

    private class AppendBlock {
        public AppendBlock(byte[] storage, int size, long offsetInStream, RequestOptions opts) {
            this.blockStorage = storage;
            this.blockSize = size;
            this.offsetInStream = offsetInStream;
            this.requestOptions = opts;
        }

        public byte[] blockStorage;
        public int blockSize;
        public long offsetInStream;
        public RequestOptions requestOptions;
    }

    // Blocks that have been successfully queued on the server side with the PIPELINE flag
    // but haven't been committed yet; these blocks might require to be retried
    private LinkedList<AppendBlock> pendingBlocks = new LinkedList<>();

    // package-private constructor - use Factory Method in AzureDataLakeStoreClient
    ADLFileOutputStream(String filename,
                        ADLStoreClient client,
                        HttpContext httpContext,
                        boolean isCreate,
                        String leaseId) throws IOException {
        this.filename = filename;
        this.client = client;
        this.isCreate = isCreate;
        this.leaseId = (leaseId == null)? UUID.randomUUID().toString() : leaseId;
        this.httpContext = httpContext;

        if (!isCreate) initializeAppendStream();

        if (log.isTraceEnabled()) {
            log.trace("ADLFIleOutputStream created for client {} for file {}, create={}", client.getClientId(), filename, isCreate);
        }
    }

    private void initializeAppendStream() throws IOException {
        boolean append0succeeded = doZeroLengthAppend(-1);  // do 0-length append with sync flag to update length
        if (!append0succeeded) {
            throw new IOException("Error doing 0-length append for append stream for file " + filename);
        }
        DirectoryEntry dirent = client.getDirectoryEntry(filename);
        if (dirent != null) {
            remoteCursor = dirent.length;
        } else {
            throw new IOException("Failure getting directoryEntry during append stream creation for file " + filename);
        }
    }

    @Override
    public void write(int b) throws IOException {
        byte buf[] = new byte[1];
        buf[0] = (byte) b;
        write(buf, 0, 1);
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (b == null) return;
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (streamClosed) throw new IOException("attempting to write to a closed stream;");
        if (b == null) {
            throw new NullPointerException();
        }
        if ((off < 0) || (off > b.length) || (len < 0) ||
                ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }

        if (off > b.length || len > (b.length - off)) throw new IllegalArgumentException("array offset and length are > array size");

        if (log.isTraceEnabled()) {
            log.trace("Stream write of size {} for client {} for file {}", len, client.getClientId(), filename);
        }

        if (buffer == null) buffer = bufferManager.getBuffer();

        // if len > 4MB, then we force-break the write into 4MB chunks
        while (len > blocksize) {
            flush(SyncFlag.PIPELINE); // flush first, because we want to preserve record
            // boundary of last append
            addToBuffer(b, off, blocksize);
            off += blocksize;
            len -= blocksize;
        }
        // now len == the remaining length

        //if adding this to buffer would overflow buffer, then flush buffer first
        if (len > buffer.length - cursor) {
            flush(SyncFlag.PIPELINE);
        }
        // now we know b will fit in remaining buffer, so just add it in
        addToBuffer(b, off, len);
    }



    private void addToBuffer(byte[] b, int off, int len) {
        if (len > buffer.length - cursor) { // if requesting to copy more than remaining space in buffer
            throw new IllegalArgumentException("invalid buffer copy requested in addToBuffer");
        }
        System.arraycopy(b, off, buffer, cursor, len);
        cursor += len;
    }

    @Override
    public void flush() throws IOException {
        flush(SyncFlag.METADATA);
    }


        private void flush(SyncFlag syncFlag) throws IOException {
            if (log.isTraceEnabled()) {
                log.trace("flush() with data size {} at offset {} for client {} for file {}", cursor, remoteCursor, client.getClientId(), filename);
            }
            // Ignoring this, because HBase actually calls flush after close() <sigh>
            if (streamClosed) return;
            if (cursor == 0 && (syncFlag==SyncFlag.DATA || syncFlag==SyncFlag.PIPELINE)) return;  // nothing to flush
            if (cursor == 0 && lastFlushUpdatedMetadata && syncFlag == SyncFlag.METADATA) return; // do not send a
                                                       // flush if the last flush updated metadata and there is no data
            if (buffer == null) buffer = bufferManager.getBuffer();
            RequestOptions opts = new RequestOptions();
            opts.retryPolicy = new NoRetryPolicy();
            OperationResponse resp = new OperationResponse();
            Core.append(filename, remoteCursor, buffer, 0, cursor, leaseId,
                    leaseId, syncFlag, client, httpContext, opts, resp);

            // If ever retry this block, use the exponential backoff policy
            opts.retryPolicy = new ExponentialBackoffPolicy();

            if (!resp.successful) {

                // Retry all pending blocks with SyncFlag.DATA
                RetryPendingBlocks();

                // Retry the current block, switching SyncFlag.PIPELINE to SyncFlag.DATA
                OperationResponse retryResp = new OperationResponse();
                Core.append(filename, remoteCursor, buffer, 0, cursor, leaseId, leaseId,
                        syncFlag == SyncFlag.PIPELINE ? SyncFlag.DATA : syncFlag, client, httpContext, opts, retryResp);

                if (!retryResp.successful) {
                    if (retryResp.numRetries > 0 && retryResp.httpResponseCode == 400 && "BadOffsetException".equals(retryResp.remoteExceptionName)) {
                        // if this was a retry and we get bad offset, then this might be because we got a transient
                        // failure on first try, but request succeeded on back-end. In that case, the retry would fail
                        // with bad offset. To detect that, we check if there was a retry done, and if the current error we
                        // have is bad offset.
                        // If so, do a zero-length append at the current expected Offset, and if that succeeds,
                        // then the file length must be good - swallow the error. If this append fails, then the last append
                        // did not succeed and we have some other offset on server - bubble up the error.
                        long expectedRemoteLength = remoteCursor + cursor;
                        boolean append0Succeeded = doZeroLengthAppend(expectedRemoteLength);
                        if (append0Succeeded) {
                            log.debug("zero-length append succeeded at expected offset (" + expectedRemoteLength + "), " +
                                    " ignoring BadOffsetException for session: " + leaseId + ", file: " + filename);
                            remoteCursor += cursor;
                            cursor = 0;
                            lastFlushUpdatedMetadata = false;
                            return;
                        } else {
                            log.debug("Append failed at expected offset(" + expectedRemoteLength +
                                    "). Bubbling exception up for session: " + leaseId + ", file: " + filename);
                        }
                    }
                    throw client.getExceptionFromResponse(retryResp, "Error appending to file " + filename);
                }
            } else {
                if (syncFlag != SyncFlag.PIPELINE) {
                    // Non pipelined append success implies all previous blocks must be committed
                    pendingBlocks.clear();
                } else {
                    // Append succeeded, free all committed blocks
                    FreeAppendBlocks(resp.committedBlockOffset);
                }
            }

            remoteCursor += cursor;
            cursor = 0;
            buffer = bufferManager.getBuffer();
            lastFlushUpdatedMetadata = (syncFlag == SyncFlag.METADATA || syncFlag == SyncFlag.CLOSE);
        }

    private void RetryPendingBlocks() throws IOException {
        ListIterator<AppendBlock> it = pendingBlocks.listIterator();
        while (it.hasNext()) {
            AppendBlock block = it.next();
            OperationResponse resp = new OperationResponse();
            Core.append(filename, block.offsetInStream, block.blockStorage, 0, block.blockSize, leaseId,
                    leaseId, SyncFlag.DATA, client, httpContext, block.requestOptions, resp);

            if (resp.successful || resp.httpResponseCode == 400 && "BadOffsetException".equals(resp.remoteExceptionName)) {
                it.remove();
                bufferManager.releaseBuffer(block.blockStorage);
            } else {
                throw client.getExceptionFromResponse(resp, "Error appending to file " + filename);
            }
        }
    }

    private void FreeAppendBlocks(long committedBlockOffset) {
        ListIterator<AppendBlock> it = pendingBlocks.listIterator();
        while (it.hasNext()) {
            AppendBlock block = it.next();
            if (block.offsetInStream <= committedBlockOffset) {
                it.remove();
                bufferManager.releaseBuffer(block.blockStorage);
            } else {
                break;
            }
        }
    }

    private boolean doZeroLengthAppend(long offset) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialBackoffPolicy();
        OperationResponse resp = new OperationResponse();
        Core.append(filename, offset, null, 0, 0, leaseId, leaseId, SyncFlag.METADATA,
            client, httpContext, opts, resp);
        return resp.successful;
    }

    /**
     * Sets the size of the internal write buffer (default is 4MB).
     *
     * @param newSize requested size of buffer
     * @throws IOException throws {@link ADLException} if there is an error
     */
    public void setBufferSize(int newSize) throws IOException {
        if (newSize <=0) throw new IllegalArgumentException("Buffer size cannot be zero or less: " + newSize);
        if (newSize == blocksize) return;  // nothing to do

        if (cursor != 0) {   // if there's data in the buffer then flush it first
            flush(SyncFlag.DATA);
        }
        blocksize = newSize;
        bufferManager.setBufferSize(newSize);
        buffer = null;
    }

    @Override
    public void close() throws IOException {
        if(streamClosed) return; // Return silently upon multiple closes
        flush(SyncFlag.CLOSE);
        streamClosed = true;
        HttpContextStore.releaseHttpContext(httpContext);
        buffer = null;   // release byte buffer so it can be GC'ed even if app continues to hold reference to stream
        if (log.isTraceEnabled()) {
            log.trace("Stream closed for client {} for file {}", client.getClientId(), filename);
        }
    }
}
