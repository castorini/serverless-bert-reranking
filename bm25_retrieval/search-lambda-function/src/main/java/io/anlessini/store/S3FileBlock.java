package io.anlessini.store;

import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.PriorityQueue;

public class S3FileBlock implements Comparable<S3FileBlock> {
  /**
   * Each S3FileBlock is 64 MB in size except for the last one which is < 64 MB
   */
  static final int DEFAULT_BLOCK_SIZE = 1024 * 1024 * 64;

  public final S3ObjectSummary summary;
  public final long blockIndex;
  public final long offset;

  public static PriorityQueue<S3FileBlock> of(S3ObjectSummary summary, long offset, int length) {
    long startIndex = offset / DEFAULT_BLOCK_SIZE;
    long endIndex = (offset + length - 1) / DEFAULT_BLOCK_SIZE;
    PriorityQueue<S3FileBlock> ret = new PriorityQueue<>();
    for (long i = startIndex; i <= endIndex; i++) {
      ret.add(new S3FileBlock(summary, i));
    }
    return ret;
  }

  public S3FileBlock(S3ObjectSummary summary, long blockIndex) {
    this.summary = summary;
    this.blockIndex = blockIndex;
    this.offset = blockIndex * DEFAULT_BLOCK_SIZE;
  }

  public int length() {
    long endOffset = Math.min(summary.getSize(), offset + DEFAULT_BLOCK_SIZE);
    return Math.toIntExact(endOffset - offset);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    S3FileBlock that = (S3FileBlock) o;

    if (blockIndex != that.blockIndex) return false;
    if (offset != that.offset) return false;
    return summary.equals(that.summary);
  }

  @Override
  public int hashCode() {
    int result = summary.hashCode();
    result = 31 * result + (int) (blockIndex ^ (blockIndex >>> 32));
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    return result;
  }

  @Override
  public int compareTo(S3FileBlock that) {
    return Long.compare(this.offset, that.offset);
  }

  @Override
  public String toString() {
    return "S3FileBlock{" +
        "summary=" + summary +
        ", blockIndex=" + blockIndex +
        ", offset=" + offset +
        '}';
  }
}
