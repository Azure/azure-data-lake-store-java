/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
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

    // package-private constructor - use Factory Method in AzureDataLakeStoreClient
    ADLFileOutputStream(String filename,
                        ADLStoreClient client,
                        boolean isCreate,
                        String leaseId) throws IOException {
        this.filename = filename;
        this.client = client;
        this.isCreate = isCreate;
        this.leaseId = (leaseId == null)? UUID.randomUUID().toString() : leaseId;

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

        if (buffer == null) buffer = new byte[blocksize];

        // if len > 4MB, then we force-break the write into 4MB chunks
        while (len > blocksize) {
            flush(SyncFlag.DATA); // flush first, because we want to preserve record
            // boundary of last append
            addToBuffer(b, off, blocksize);
            off += blocksize;
            len -= blocksize;
        }
        // now len == the remaining length

        //if adding this to buffer would overflow buffer, then flush buffer first
        if (len > buffer.length - cursor) {
            flush(SyncFlag.DATA);
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
            if (cursor == 0 && (syncFlag==SyncFlag.DATA)) return;  // nothing to flush
            if (cursor == 0 && lastFlushUpdatedMetadata && syncFlag == SyncFlag.METADATA) return; // do not send a
                                                       // flush if the last flush updated metadata and there is no data
            if (buffer == null) buffer = new byte[blocksize];
            RequestOptions opts = new RequestOptions();
            opts.retryPolicy = new ExponentialBackoffPolicy();
            opts.timeout = client.timeout + (1000 + (buffer.length / 1000 / 1000)); // 1 second grace per MB to upload
            OperationResponse resp = new OperationResponse();
            Core.append(filename, remoteCursor, buffer, 0, cursor, leaseId,
                    leaseId, syncFlag, client, opts, resp);
            if (!resp.successful) {
                if (resp.numRetries > 0 && resp.httpResponseCode == 400 && "BadOffsetException".equals(resp.remoteExceptionName)) {
                    // if this was a retry and we get bad offset, then this might be because we got a transient
                    // failure on first try, but request succeeded on back-end. In that case, the retry would fail
                    // with bad offset. To detect that, we check if there was a retry done, and if the current error we
                    // have is bad offset.
                    // If so, do a zero-length append at the current expected Offset, and if that succeeds,
                    // then the file length must be good - swallow the error. If this append fails, then the last append
                    // did not succeed and we have some other offset on server - bubble up the error.
                    long expectedRemoteLength = remoteCursor + cursor;
                    boolean append0Succeeded =  doZeroLengthAppend(expectedRemoteLength);
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
                throw client.getExceptionFromResponse(resp, "Error appending to file " + filename);
            }
            remoteCursor += cursor;
            cursor = 0;
            lastFlushUpdatedMetadata = (syncFlag != SyncFlag.DATA);
        }

    private boolean doZeroLengthAppend(long offset) throws IOException {
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = new ExponentialBackoffPolicy();
        OperationResponse resp = new OperationResponse();
        Core.append(filename, offset, null, 0, 0, leaseId, leaseId, SyncFlag.METADATA,
            client, opts, resp);
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
        buffer = null;
    }

    @Override
    public void close() throws IOException {
        if(streamClosed) return; // Return silently upon multiple closes
        flush(SyncFlag.CLOSE);
        streamClosed = true;
        buffer = null;   // release byte buffer so it can be GC'ed even if app continues to hold reference to stream
        if (log.isTraceEnabled()) {
            log.trace("Stream closed for client {} for file {}", client.getClientId(), filename);
        }
    }
}
