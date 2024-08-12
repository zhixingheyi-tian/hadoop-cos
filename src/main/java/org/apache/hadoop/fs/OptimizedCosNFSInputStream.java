package org.apache.hadoop.fs;

import com.qcloud.cos.model.COSObjectInputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutorService;

import static org.apache.hadoop.fs.CosNConfigKeys.*;

public class OptimizedCosNFSInputStream extends CosNFSInputStream {

    private long readahead = CosNConfigKeys.DEFAULT_READAHEAD_RANGE;
    private final String uri;
    private COSObjectInputStream wrappedStream;
    private COSNInputPolicy inputPolicy;

    /**
     * The end of the content range of the last request.
     * This is an absolute value of the range, not a length field.
     */
    private long contentRangeFinish;

    /**
     * The start of the content range of the last request.
     */
    private long contentRangeStart;

    /**
     * Input Stream
     *
     * @param conf                     config
     * @param store                    native file system
     * @param statistics               statis
     * @param key                      cos key
     * @param fileSize                 file size
     * @param readAheadExecutorService thread executor
     */
    public OptimizedCosNFSInputStream(Configuration conf, NativeFileSystemStore store, FileSystem.Statistics statistics, String bucket, String key, long fileSize, ExecutorService readAheadExecutorService) {
        super(conf, store, statistics, key, fileSize, readAheadExecutorService);
        this.uri = "cosn://" + bucket + "/" + this.key;
        this.inputPolicy = COSNInputPolicy.getPolicy(
                conf.getTrimmed(INPUT_FADVISE, INPUT_FADV_NORMAL));
    }

    /**
     * Set/update the input policy of the stream.
     * This updates the stream statistics.
     * @param inputPolicy new input policy.
     */
    private void setInputPolicy(COSNInputPolicy inputPolicy) {
        this.inputPolicy = inputPolicy;
    }

    @Override
    public synchronized void seek(long targetPos) throws IOException {
        this.checkOpened();

        // Do not allow negative seek
        if (targetPos < 0) {
            throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK
                    + " " + targetPos);
        }

        if (this.fileSize <= 0) {
            return;
        }

        // Lazy seek
        nextPos = targetPos;
    }

    /**
     * Seek without raising any exception. This is for use in
     * {@code finally} clauses
     * @param positiveTargetPos a target position which must be positive.
     */
    private void seekQuietly(long positiveTargetPos) {
        try {
            seek(positiveTargetPos);
        } catch (IOException ioe) {
            LOG.debug("Ignoring IOE on seek of {} to {}",
                    uri, positiveTargetPos, ioe);
        }
    }

    /**
     * Perform lazy seek and adjust stream to correct position for reading.
     *
     * @param targetPos position from where data should be read
     * @param len length of the content that needs to be read
     */
    private void lazySeek(long targetPos, long len) throws IOException {
        //For lazy seek
        seekInStream(targetPos, len);

        //re-open at specific location if needed
        if (wrappedStream == null) {
            reopen("read from new offset", targetPos, len, false);
        }
    }

    /**
     * Opens up the stream at specified target position and for given length.
     *
     * @param reason reason for reopen
     * @param targetPos target position
     * @param length length requested
     * @throws IOException on any failure to open the object
     */
    private synchronized void reopen(String reason, long targetPos, long length,
                                     boolean forceAbort) throws IOException {

        if (wrappedStream != null) {
            closeStream("reopen(" + reason + ")", contentRangeFinish, forceAbort);
        }

        contentRangeFinish = calculateRequestLimit(inputPolicy, targetPos,
                length, fileSize, readahead);
        LOG.debug("reopen({}) for {} range[{}-{}], length={}," +
                        " streamPosition={}, nextReadPosition={}, policy={}",
                uri, reason, targetPos, contentRangeFinish, length,  position, nextPos,
                inputPolicy);

        wrappedStream = (COSObjectInputStream) this.store.retrieveBlock(
                this.key, targetPos,
                contentRangeFinish - 1);

        contentRangeStart = targetPos;
        if (wrappedStream == null) {
            throw new PathIOException(uri,
                    "Null IO stream, "  + " (" + reason +  ") ");
        }

        this.position = targetPos;
    }

    /**
     * Calculate the limit for a get request, based on input policy
     * and state of object.
     * @param inputPolicy input policy
     * @param targetPos position of the read
     * @param length length of bytes requested; if less than zero "unknown"
     * @param contentLength total length of file
     * @param readahead current readahead value
     * @return the absolute value of the limit of the request.
     */
    static long calculateRequestLimit(
            COSNInputPolicy inputPolicy,
            long targetPos,
            long length,
            long contentLength,
            long readahead) {
        long rangeLimit;
        switch (inputPolicy) {
            case Random:
                // positioned.
                // read either this block, or the here + readahead value.
                rangeLimit = (length < 0) ? contentLength
                        : targetPos + Math.max(readahead, length);
                break;

            case Sequential:
                // sequential: plan for reading the entire object.
                rangeLimit = contentLength;
                break;

            case Normal:
                // normal is considered sequential until a backwards seek switches
                // it to 'Random'
            default:
                rangeLimit = contentLength;

        }
        // cannot read past the end of the object
        rangeLimit = Math.min(contentLength, rangeLimit);
        return rangeLimit;
    }

    private void closeStream(String reason, long length, boolean forceAbort) {
        if (wrappedStream != null) {

            // if the amount of data remaining in the current request is greater
            // than the readahead value: abort.
            long remaining = remainingInCurrentRequest();
            LOG.debug("Closing stream {}: {}", reason,
                    forceAbort ? "abort" : "soft");
            boolean shouldAbort = forceAbort || remaining > readahead;
            if (!shouldAbort) {
                try {
                    // clean close. This will read to the end of the stream,
                    // so, while cleaner, can be pathological on a multi-GB object

                    // explicitly drain the stream
                    long drained = 0;
                    while (wrappedStream.read() >= 0) {
                        drained++;
                    }
                    LOG.debug("Drained stream of {} bytes", drained);

                    // now close it
                    wrappedStream.close();
                } catch (IOException e) {
                    // exception escalates to an abort
                    LOG.debug("When closing {} stream for {}", uri, reason, e);
                    shouldAbort = true;
                }
            }
            if (shouldAbort) {
                // Abort, rather than just close, the underlying stream.  Otherwise, the
                // remaining object payload is read from S3 while closing the stream.
                LOG.debug("Aborting stream");
                wrappedStream.abort();
            }
            LOG.debug("Stream {} {}: {}; remaining={} streamPos={},"
                            + " nextReadPos={}," +
                            " request range {}-{} length={}",
                    uri, (shouldAbort ? "aborted" : "closed"), reason,
                    remaining, position, nextPos,
                    contentRangeStart, contentRangeFinish,
                    length);
            wrappedStream = null;
        }
    }

    /**
     * Bytes left in the current request.
     * Only valid if there is an active request.
     * @return how many bytes are left to read in the current GET.
     */
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public synchronized long remainingInCurrentRequest() {
        return this.contentRangeFinish - this.position;
    }



    /**
     * Adjust the stream to a specific position.
     *
     * @param targetPos target seek position
     * @param length length of content that needs to be read from targetPos
     * @throws IOException
     */
    private void seekInStream(long targetPos, long length) throws IOException {
        this.checkOpened();
        if (wrappedStream == null) {
            return;
        }
        // compute how much more to skip
        long diff = targetPos - position;
        if (diff > 0) {
            // forward seek -this is where data can be skipped

            int available = wrappedStream.available();
            // always seek at least as far as what is available
            long forwardSeekRange = Math.max(readahead, available);
            // work out how much is actually left in the stream
            // then choose whichever comes first: the range or the EOF
            long remainingInCurrentRequest = remainingInCurrentRequest();

            long forwardSeekLimit = Math.min(remainingInCurrentRequest,
                    forwardSeekRange);
            boolean skipForward = remainingInCurrentRequest > 0
                    && diff < forwardSeekLimit;
            if (skipForward) {
                // the forward seek range is within the limits
                LOG.debug("Forward seek on {}, of {} bytes", uri, diff);
                long skipped = wrappedStream.skip(diff);
                if (skipped > 0) {
                    position += skipped;
                }

                if (position == targetPos) {
                    // all is well
                    LOG.debug("Now at {}: bytes remaining in current request: {}",
                            position, remainingInCurrentRequest());
                    return;
                } else {
                    // log a warning; continue to attempt to re-open
                    LOG.warn("Failed to seek on {} to {}. Current position {}",
                            uri, targetPos,  position);
                }
            }
        } else if (diff < 0) {
            // if the stream is in "Normal" mode, switch to random IO at this
            // point, as it is indicative of columnar format IO
            if (inputPolicy.equals(COSNInputPolicy.Normal)) {
                LOG.info("Switching to Random IO seek policy");
                setInputPolicy(COSNInputPolicy.Random);
            }
        } else {
            // targetPos == pos
            if (remainingInCurrentRequest() > 0) {
                // if there is data left in the stream, keep going
                return;
            }

        }

        // if the code reaches here, the stream needs to be reopened.
        // close the stream; if read the object will be opened at the new pos
        closeStream("seekInStream()", this.contentRangeFinish, false);
        position = targetPos;
    }

    @Override
    public synchronized int read() throws IOException {
        checkOpened();
        if (this.fileSize == 0 || (nextPos >= fileSize)) {
            return -1;
        }

        try {
            lazySeek(nextPos, 1);
        } catch (EOFException e) {
            return -1;
        }

        int byteRead =0;
        try {
            byteRead = wrappedStream.read();
        } catch (EOFException e) {
            return -1;
        } catch (SocketTimeoutException e) {
            onReadFailure(e, 1, true);
            byteRead = wrappedStream.read();
        } catch (IOException e) {
            onReadFailure(e, 1, false);
            byteRead = wrappedStream.read();
        }

        if (byteRead >= 0) {
            position++;
            nextPos++;
        }

        if (byteRead >= 0) {
            this.statistics.incrementBytesRead(1);
        }
        return byteRead;
    }

    /**
     * {@inheritDoc}
     *
     * This updates the statistics on read operations started and whether
     * or not the read operation "completed", that is: returned the exact
     * number of bytes requested.
     * @throws IOException if there are other problems
     */
    @Override
    public synchronized int read(byte[] buf, int off, int len)
            throws IOException {
        this.checkOpened();

        validatePositionedReadArgs(nextPos, buf, off, len);
        if (len == 0) {
            return 0;
        }

        if (this.fileSize == 0 || (nextPos >= fileSize)) {
            return -1;
        }

        int realLen = len;
        if ((nextPos + len) > fileSize) {
            realLen = (int)(fileSize - nextPos);
        }

        try {
            lazySeek(nextPos, realLen);
        } catch (EOFException e) {
            // the end of the file has moved
            return -1;
        }



        try {
            IOUtils.readFully(wrappedStream, buf, off, realLen);
        } catch (EOFException e) {
            // the base implementation swallows EOFs.
            return -1;
        } catch (SocketTimeoutException e) {
            onReadFailure(e, len, true);
            IOUtils.readFully(wrappedStream, buf, off, realLen);
        } catch (IOException e) {
            onReadFailure(e, len, false);
            IOUtils.readFully(wrappedStream, buf, off, realLen);
        }


        if (realLen > 0) {
            position += realLen;
            nextPos += realLen;
        }
        this.statistics.incrementBytesRead(realLen);
        return realLen;
    }

    /**
     * Subclass {@code readFully()} operation which only seeks at the start
     * of the series of operations; seeking back at the end.
     *
     * This is significantly higher performance if multiple read attempts are
     * needed to fetch the data, as it does not break the HTTP connection.
     *
     * To maintain thread safety requirements, this operation is synchronized
     * for the duration of the sequence.
     * {@inheritDoc}
     *
     */
    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException {
        checkOpened();
        validatePositionedReadArgs(position, buffer, offset, length);
        if (length == 0) {
            return;
        }
        int nread = 0;
        synchronized (this) {
            long oldPos = getPos();
            try {
                seek(position);
                while (nread < length) {
                    int nbytes = read(buffer, offset + nread, length - nread);
                    if (nbytes < 0) {
                        throw new EOFException(FSExceptionMessages.EOF_IN_READ_FULLY);
                    }
                    nread += nbytes;
                }
            } finally {
                seekQuietly(oldPos);
            }
        }
    }

    /**
     * Handle an IOE on a read by attempting to re-open the stream.
     * The filesystem's readException count will be incremented.
     * @param ioe exception caught.
     * @param length length of data being attempted to read
     * @throws IOException any exception thrown on the re-open attempt.
     */
    private void onReadFailure(IOException ioe, int length, boolean forceAbort)
            throws IOException {

        LOG.info("Got exception while trying to read from stream {}" +
                " trying to recover: " + ioe, uri);
        reopen("failure recovery", position, length, forceAbort);
    }

    /**
     * Close the stream.
     * This triggers publishing of the stream statistics back to the filesystem
     * statistics.
     * This operation is synchronized, so that only one thread can attempt to
     * close the connection; all later/blocked calls are no-ops.
     * @throws IOException on any problem
     */
    @Override
    public synchronized void close() throws IOException {
        if (this.closed.get()) {
            return;
        }
        closeStream("close() operation", this.contentRangeFinish, false);
        // this is actually a no-op
        super.close();
        this.closed.set(true);
    }
}
