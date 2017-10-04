/*
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 * See License.txt in the project root for license information.
 */

package com.microsoft.azure.datalake.store;

import com.microsoft.azure.datalake.store.retrypolicies.ExponentialBackoffPolicy;
import com.microsoft.azure.datalake.store.retrypolicies.NoRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;


/**
 * ADLFileInputStream can be used to read data from an open file on ADL.
 * It is a buffering stream, that reads data from the server in bulk, and then
 * satisfies user reads from the buffer. Default buffer size is 4MB.
 *
 * <P>
 * <B>Thread-safety</B>: Note that methods in this class are NOT thread-safe.
 * </P>
 *
 *
 */
public class ADLFileInputStream extends InputStream {
    private static final Logger log = LoggerFactory.getLogger("com.microsoft.azure.datalake.store.ADLFileInputStream");

    private final String filename;
    private final ADLStoreClient client;
    private final DirectoryEntry directoryEntry;
    private final String sessionId = UUID.randomUUID().toString();
    private static final int defaultQueueDepth = 4;  // need to experiment to see what is a good number

    private int blocksize = 4 * 1024 * 1024; // 4MB default buffer size
    private byte[] buffer = null;            // will be initialized on first use
    private int readAheadQueueDepth;         // initialized in constructor

    private long fCursor = 0;  // cursor of buffer within file - offset of next byte to read from remote server
    private int bCursor = 0;   // cursor of read within buffer - offset of next byte to be returned from buffer
    private int limit = 0;     // offset of next byte to be read into buffer from service (i.e., upper marker+1
    //                                                      of valid bytes in buffer)
    private boolean streamClosed = false;


    // no public constructor - use Factory Method in AzureDataLakeStoreClient
    ADLFileInputStream(String filename, DirectoryEntry de, ADLStoreClient client) {
        super();
        this.filename = filename;
        this.client = client;
        this.directoryEntry = de;
        int requestedQD = client.getReadAheadQueueDepth();
        this.readAheadQueueDepth = (requestedQD >= 0) ? requestedQD : defaultQueueDepth;
        if (log.isTraceEnabled()) {
            log.trace("ADLFIleInputStream created for client {} for file {}", client.getClientId(), filename);
        }
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int i = read(b, 0, 1);
        if (i<0) return i;
        else return (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (b == null) {
            throw new IllegalArgumentException("null byte array passed in to read() method");
        }
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (streamClosed) throw new IOException("attempting to read from a closed stream");
        if (b == null) {
            throw new IllegalArgumentException("null byte array passed in to read() method");
        }

        if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        }
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.read(b,off,{}) at offset {} using client {} from file {}", len, getPos(), client.getClientId(), filename);
        }

        if (len == 0) {
            return 0;
        }

        //If buffer is empty, then fill the buffer. If EOF, then return -1
        if (bCursor == limit)
        {
            if (readFromService() < 0) return -1;
        }

        //If there is anything in the buffer, then return lesser of (requested bytes) and (bytes in buffer)
        //(bytes returned may be less than requested)
        int bytesRemaining = limit - bCursor;
        int bytesToRead = Math.min(len, bytesRemaining);
        System.arraycopy(buffer, bCursor, b, off, bytesToRead);
        bCursor += bytesToRead;
        return bytesToRead;
    }

    /**
     * Read from service attempts to read {@code blocksize} bytes from service.
     * Returns how many bytes are actually read, could be less than blocksize.
     *
     * @return number of bytes actually read
     * @throws ADLException if error
     */
    protected long readFromService() throws IOException {
        if (bCursor < limit) return 0; //if there's still unread data in the buffer then dont overwrite it
        if (fCursor >= directoryEntry.length) return -1; // At or past end of file

        if (directoryEntry.length <= blocksize)
            return slurpFullFile();

        //reset buffer to initial state - i.e., throw away existing data
        bCursor = 0;
        limit = 0;
        if (buffer == null) buffer = new byte[blocksize];

        int bytesRead = readInternal(fCursor, buffer, 0, blocksize, false);
        limit += bytesRead;
        fCursor += bytesRead;
        return bytesRead;
    }


    /**
     * Reads the whole file into buffer. Useful when reading small files.
     *
     * @return number of bytes actually read
     * @throws IOException throws IOException if there is an error
     */
    protected long slurpFullFile() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.slurpFullFile() - using client {} from file {}. At offset {}", client.getClientId(), filename, getPos());
        }

        if (buffer == null) {
            blocksize = (int) directoryEntry.length;
            buffer = new byte[blocksize];
        }

        //reset buffer to initial state - i.e., throw away existing data
        bCursor = (int) getPos();  // preserve current file offset (may not be 0 if app did a seek before first read)
        limit = 0;
        fCursor = 0;  // read from beginning
        int loopCount = 0;

        // if one OPEN request doesnt get full file, then read again at fCursor
        while (fCursor < directoryEntry.length) {
            int bytesRead = readInternal(fCursor, buffer, limit, blocksize - limit, true);
            limit += bytesRead;
            fCursor += bytesRead;

            // just to be defensive against infinite loops
            loopCount++;
            if (loopCount >= 10) { throw new IOException("Too many attempts in reading whole file " + filename); }
        }
        return fCursor;
    }

    /**
     * Read upto the specified number of bytes, from a given
     * position within a file, and return the number of bytes read. This does not
     * change the current offset of a file.
     *
     * @param position position in file to read from
     * @param b  byte[] buffer to read into
     * @param offset offset into the byte buffer at which to read the data into
     * @param length number of bytes to read
     * @return the number of bytes actually read, which could be less than the bytes requested. If the {@code position}
     *         is at or after end of file, then -1 is returned.
     * @throws IOException thrown if there is an error in reading
     */
    public int read(long position, byte[] b, int offset, int length)
            throws IOException {
        if (streamClosed) throw new IOException("attempting to read from a closed stream");
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream positioned read() - at offset {} using client {} from file {}", position, client.getClientId(), filename);
        }
        return readInternal(position, b, offset, length, true);
    }

    private int readInternal(long position, byte[] b, int offset, int length, boolean bypassReadAhead) throws IOException {
        boolean readAheadEnabled = true;
        if (readAheadEnabled && !bypassReadAhead && !client.disableReadAheads) {
            // try reading from read-ahead
            if (offset != 0) throw new IllegalArgumentException("readahead buffers cannot have non-zero buffer offsets");
            int receivedBytes;

            // queue read-aheads
            int numReadAheads = this.readAheadQueueDepth;
            long nextSize;
            long nextOffset = position;
            while (numReadAheads > 0 && nextOffset < directoryEntry.length) {
                nextSize = Math.min( (long)blocksize, directoryEntry.length-nextOffset);
                if (log.isTraceEnabled())
                    log.trace("Queueing readAhead for file " + filename + " offset " + nextOffset + " thread " + Thread.currentThread().getName());
                ReadBufferManager.getBufferManager().queueReadAhead(this, nextOffset, (int) nextSize);
                nextOffset = nextOffset + nextSize;
                numReadAheads--;
            }

            // try reading from buffers first
            receivedBytes = ReadBufferManager.getBufferManager().getBlock(this, position, length, b);
            if (receivedBytes > 0) return receivedBytes;

            // got nothing from read-ahead, do our own read now
            receivedBytes = readRemote(position, b, offset, length, false);
            return receivedBytes;
        } else {
            return readRemote(position, b, offset, length, false);
        }
    }

    int readRemote(long position, byte[] b, int offset, int length, boolean speculative) throws IOException {
        if (position < 0) throw new IllegalArgumentException("attempting to read from negative offset");
        if (position >= directoryEntry.length) return -1;  // Hadoop prefers -1 to EOFException
        if (b == null) throw new IllegalArgumentException("null byte array passed in to read() method");
        if (offset >= b.length) throw new IllegalArgumentException("offset greater than length of array");
        if (length < 0) throw new IllegalArgumentException("requested read length is less than zero");
        if (length > (b.length - offset))
            throw new IllegalArgumentException("requested read length is more than will fit after requested offset in buffer");

        byte[] junkbuffer = new byte[16*1024];
        RequestOptions opts = new RequestOptions();
        opts.retryPolicy = speculative ? new NoRetryPolicy() : new ExponentialBackoffPolicy();
        OperationResponse resp = new OperationResponse();
        InputStream inStream = Core.open(filename, position, length, sessionId, speculative, client, opts, resp);
        if (speculative && !resp.successful && resp.httpResponseCode == 400 && resp.remoteExceptionName.equals("SpeculativeReadNotSupported")) {
            client.disableReadAheads = true;
            return 0;
        }
        if (!resp.successful) throw client.getExceptionFromResponse(resp, "Error reading from file " + filename);
        if (resp.responseContentLength == 0 && !resp.responseChunked) return 0;  //Got nothing
        int bytesRead;
        int totalBytesRead = 0;
        long start = System.nanoTime();
        try {
            do {
                bytesRead = inStream.read(b, offset + totalBytesRead, length - totalBytesRead);
                if (bytesRead > 0) { // if not EOF of the Core.open's stream
                    totalBytesRead += bytesRead;
                }
            } while (bytesRead >= 0 && totalBytesRead < length);
            if (bytesRead >= 0) {  // read to EOF on the stream, so connection can be reused
                while (inStream.read(junkbuffer, 0, junkbuffer.length)>=0); // read and consume rest of stream, if any remains
            }
        } catch (IOException ex) {
            throw new ADLException("Error reading data from response stream in positioned read() for file " + filename, ex);
        } finally {
            if (inStream != null) inStream.close();
            long timeTaken=(System.nanoTime() - start)/1000000;
            if (log.isDebugEnabled()) {
                String logline ="HTTPRequestRead," + (resp.successful?"Succeeded":"Failed") +
                        ",cReqId:" + opts.requestid +
                        ",lat:" + Long.toString(resp.lastCallLatency+timeTaken) +
                        ",Reqlen:" + totalBytesRead +
                        ",sReqId:" + resp.requestId +
                        ",path:" + filename +
                        ",offset:" + position;
                log.debug(logline);
            }
        }
        return totalBytesRead;
    }


    /**
     * Seek to given position in stream.
     * @param n position to seek to
     * @throws IOException if there is an error
     * @throws EOFException if attempting to seek past end of file
     */
    public void seek(long n) throws IOException, EOFException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.seek({}) using client {} for file {}", n, client.getClientId(), filename);
        }
        if (streamClosed) throw new IOException("attempting to seek into a closed stream;");
        if (n<0) throw new EOFException("Cannot seek to before the beginning of file");
        if (n>directoryEntry.length) throw new EOFException("Cannot seek past end of file");

        if (n>=fCursor-limit && n<=fCursor) { // within buffer
            bCursor = (int) (n-(fCursor-limit));
            return;
        }

        // next read will read from here
        fCursor = n;

        //invalidate buffer
        limit = 0;
        bCursor = 0;
    }

    @Override
    public long skip(long n) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.skip({}) using client {} for file {}", n, client.getClientId(), filename);
        }
        if (streamClosed) throw new IOException("attempting to skip() on a closed stream");
        long currentPos = getPos();
        long newPos = currentPos + n;
        if (newPos < 0) {
            newPos = 0;
            n = newPos - currentPos;
        }
        if (newPos > directoryEntry.length) {
            newPos = directoryEntry.length;
            n = newPos - currentPos;
        }
        seek(newPos);
        return n;
    }

    /**
     * Sets the size of the internal read buffer (default is 4MB).
     * @param newSize requested size of buffer
     * @throws ADLException if there is an error
     */
    public void setBufferSize(int newSize) throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.setBufferSize({}) using client {} for file {}", newSize, client.getClientId(), filename);
        }
        if (newSize <=0) throw new IllegalArgumentException("Buffer size cannot be zero or less: " + newSize);
        if (newSize == blocksize) return;  // nothing to do

        // discard existing buffer.
        // We could write some code to keep what we can from existing buffer, but given this call will
        // be rarely used, and even when used will likely be right after the stream is constructed,
        // the extra complexity is not worth it.
        unbuffer();
        blocksize = newSize;
        buffer = null;
    }

    /**
     * Sets the Queue depth to be used for read-aheads in this stream.
     *
     * @param queueDepth the desired queue depth, set to 0 to disable read-ahead
     */
    public void setReadAheadQueueDepth(int queueDepth) {
        if (queueDepth < 0) throw new IllegalArgumentException("Queue depth has to be 0 or more");
        this.readAheadQueueDepth = queueDepth;
    }

    /**
     * returns the remaining number of bytes available to read from the buffer, without having to call
     * the server
     *
     * @return the number of bytes availabel
     * @throws IOException throws {@link ADLException} if call fails
     */
    @Override
    public int available() throws IOException {
        if (streamClosed) throw new IOException("attempting to call available() on a closed stream");
        return limit - bCursor;
    }

    /**
     * Returns the length of the file that this stream refers to. Note that the length returned is the length
     * as of the time the Stream was opened. Specifically, if there have been subsequent appends to the file,
     * they wont be reflected in the returned length.
     *
     * @return length of the file.
     * @throws IOException if the stream is closed
     */
    public long length() throws IOException {
        if (streamClosed) throw new IOException("attempting to call length() on a closed stream");
        return directoryEntry.length;
    }

    /**
     * gets the position of the cursor within the file
     * @return position of the cursor
     * @throws IOException throws {@link IOException} if there is an error
     */
    public long getPos() throws IOException {
        if (streamClosed) throw new IOException("attempting to call getPos() on a closed stream");
        return fCursor - limit + bCursor;
    }

    /**
     * invalidates the buffer. The next read will fetch data from server.
     * @throws IOException throws {@link IOException} if there is an error
     */
    public void unbuffer() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.unbuffer() for client {} for file {}", client.getClientId(), filename);
        }
        fCursor = getPos();
        limit = 0;
        bCursor = 0;
    }

    @Override
    public void close() throws IOException {
        if (log.isTraceEnabled()) {
            log.trace("ADLFileInputStream.close() for client {} for file {}", client.getClientId(), filename);
        }
        streamClosed = true;
        buffer = null; // de-reference the buffer so it can be GC'ed sooner
    }


    public String getFilename() {
        return this.filename;
    }

    /**
     * Not supported by this stream. Throws {@link UnsupportedOperationException}
     * @param readlimit ignored
     */
    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
    }

    /**
     * Not supported by this stream. Throws {@link UnsupportedOperationException}
     */
    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException("mark()/reset() not supported on this stream");
    }

    /**
     * gets whether mark and reset are supported by {@code ADLFileInputStream}. Always returns false.
     *
     * @return always {@code false}
     */
    @Override
    public boolean markSupported() {
        return false;
    }
}
