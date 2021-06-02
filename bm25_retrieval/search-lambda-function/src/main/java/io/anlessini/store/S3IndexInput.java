package io.anlessini.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BufferedIndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;

public class S3IndexInput extends BufferedIndexInput {
  private static final Logger LOG = LogManager.getLogger(S3IndexInput.class);
  /**
   * The size of the buffer used by BufferedIndexInput, default to 4 MB
   */
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 4;

  public static class ReadStats {
    public final AtomicLong readTotal = new AtomicLong();

    public final AtomicLong readFromS3 = new AtomicLong();
  }

  public static final ReadStats stats = new ReadStats();

  private final AmazonS3 s3Client;
  private final S3ObjectSummary summary;
  private final S3BlockCache cache;

  /**
   * The start offset in the entire file, non-zero in the slice case
   */
  private final long off;
  /**
   * The end offset
   */
  private final long end;

  public S3IndexInput(AmazonS3 s3Client, S3ObjectSummary summary) {
    this(s3Client, summary, 0, summary.getSize(), defaultBufferSize(summary.getSize()));
  }

  public S3IndexInput(AmazonS3 s3Client, S3ObjectSummary summary, long offset, long length, int bufferSize) {
    super(summary.getBucketName() + "/" + summary.getKey(), bufferSize);
    this.s3Client = s3Client;
    this.cache = S3BlockCache.getInstance();
    this.summary = summary;
    this.off = offset;
    this.end = offset + length;
    LOG.trace("Opened S3IndexInput " + toString() + "@" + hashCode() + " , bufferSize=" + getBufferSize());
  }

  private static int defaultBufferSize(long fileLength) {
    long bufferSize = fileLength;
    bufferSize = Math.max(bufferSize, MIN_BUFFER_SIZE);
    bufferSize = Math.min(bufferSize, DEFAULT_BUFFER_SIZE);
    return Math.toIntExact(bufferSize);
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public long length() {
    return end - off;
  }

  @Override
  public S3IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > this.length()) {
      throw new IllegalArgumentException("Slice " + sliceDescription + " out of bounds: " +
          "offset=" + offset + ",length=" + length + ",fileLength=" + this.length() + ": " + toString());
    }
    LOG.trace("[slice][" + toString() + "@" + hashCode() + "] " + getFullSliceDescription(sliceDescription) + ", offset=" + offset + ", length=" + length + ", fileLength=" + this.length());
    return new S3IndexInput(s3Client, summary, off + offset, length, defaultBufferSize(length));
  }

  @Override
  public S3IndexInput clone() {
    S3IndexInput clone = (S3IndexInput) super.clone();
    LOG.trace("[clone][" + toString() + "@" + hashCode() + "], clone=" + clone.hashCode());
    return clone;
  }

  @Override
  protected void readInternal(byte[] dst, final int offset, final int length) throws IOException {
    final long startPos = getFilePointer() + this.off;
    final long endPos = startPos + length;

    if (startPos + length > end) {
      throw new EOFException("reading past EOF: " + toString() + "@" + hashCode());
    }

    LOG.trace("[read][" + summary.getKey() + "] @" + startPos + ":" + length);
    final PriorityQueue<S3FileBlock> fileBlocks = S3FileBlock.of(summary, startPos, length);
    final Map<S3FileBlock, byte[]> cacheBlocks = new HashMap<>();
    final MinMaxPriorityQueue<S3FileBlock> cacheMisses = MinMaxPriorityQueue.create();
    for (S3FileBlock fb : fileBlocks) {
      cacheBlocks.put(fb, cache.getBlock(fb));
      if (cacheBlocks.get(fb) == null) {
        cacheMisses.add(fb);
      }
    }

    if (!cacheMisses.isEmpty()) {
      long downloadStartOffset = cacheMisses.peekFirst().offset;
      long downloadEndOffset = cacheMisses.peekLast().offset + cacheMisses.peekLast().length();
      downloadEndOffset = Math.min(summary.getSize(), downloadEndOffset);
      int downloadLength = Math.toIntExact(downloadEndOffset - downloadStartOffset);
      PriorityQueue<S3FileBlock> downloadBlocks = S3FileBlock.of(summary, downloadStartOffset, downloadLength);

      LOG.trace("[readFromS3][" + summary.getKey() + "] @" + downloadStartOffset + ":" + downloadLength);
      GetObjectRequest rangeObjectRequest = new GetObjectRequest(summary.getBucketName(), summary.getKey())
          .withRange(downloadStartOffset, downloadEndOffset - 1);
      S3Object object = s3Client.getObject(rangeObjectRequest);
      stats.readFromS3.addAndGet(downloadLength);

      for (S3FileBlock fb : downloadBlocks) {
        byte[] data = new byte[fb.length()];
        int bytesRead = IOUtils.read(object.getObjectContent(), data);
        if (bytesRead != fb.length()) {
          throw new IOException("block is not completely filled! fb=" + fb + " bytesRead=" + bytesRead);
        }

        cache.cacheBlock(fb, data);
        cacheBlocks.put(fb, data);
      }

      object.close();
    }

    int bytesRead = 0;
    int dstOffset = offset;
    for (S3FileBlock fb : fileBlocks) {
      byte[] src = cacheBlocks.get(fb);
      long blockStart = fb.offset, blockEnd = fb.offset + fb.length();
      int toRead = Math.toIntExact(Math.min(blockEnd, endPos) - Math.max(blockStart, startPos));
      int srcOffset = Math.toIntExact(Math.max(0, startPos - blockStart));
      System.arraycopy(src, srcOffset, dst, dstOffset, toRead);

      dstOffset += toRead;
      bytesRead += toRead;

      stats.readTotal.addAndGet(toRead);
    }

    if (bytesRead != length) {
      throw new IOException("read is not fulfilled completely!" + toString()
          + " offset=" + offset + " length=" + length + " bytesRead=" + bytesRead);
    }
  }

  @Override
  protected void seekInternal(long pos) throws IOException {
    if (pos > length()) {
      throw new EOFException("read past EOF: pos=" + pos + ", length=" + length() + ": " + toString() + "@" + hashCode());
    }
  }

  public static void logStats() {
    LOG.trace("Total bytes read from S3: " + stats.readFromS3.get()
        + ", total bytes read: " + stats.readTotal.get());
  }

  public static void clearStats() {
    stats.readTotal.set(0);
    stats.readFromS3.set(0);
  }
}
