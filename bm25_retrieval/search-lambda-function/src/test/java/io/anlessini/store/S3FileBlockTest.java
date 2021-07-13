package io.anlessini.store;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.Test;

import java.util.List;

import static io.anlessini.store.S3FileBlock.DEFAULT_BLOCK_SIZE;
import static org.junit.Assert.*;

public class S3FileBlockTest {

  @Test
  public void testOf() {
    S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName("foo");
    summary.setKey("bar");
    summary.setSize(4 * DEFAULT_BLOCK_SIZE + DEFAULT_BLOCK_SIZE / 2); // 288 MB

    List<S3FileBlock> fb = List.copyOf(S3FileBlock.of(summary, 0, DEFAULT_BLOCK_SIZE));
    assertEquals(1, fb.size());
    assertEquals(0, fb.get(0).blockIndex);
    assertEquals(0, fb.get(0).offset);
    assertEquals(DEFAULT_BLOCK_SIZE, fb.get(0).length());

    fb = List.copyOf(S3FileBlock.of(summary, 0, DEFAULT_BLOCK_SIZE * 2));
    assertEquals(2, fb.size());
    assertEquals(0, fb.get(0).blockIndex);
    assertEquals(0, fb.get(0).offset);
    assertEquals(DEFAULT_BLOCK_SIZE, fb.get(0).length());
    assertEquals(1, fb.get(1).blockIndex);
    assertEquals(DEFAULT_BLOCK_SIZE, fb.get(1).offset);
    assertEquals(DEFAULT_BLOCK_SIZE, fb.get(1).length());

    fb = List.copyOf(S3FileBlock.of(summary, DEFAULT_BLOCK_SIZE * 3 / 2, DEFAULT_BLOCK_SIZE));
    assertEquals(2, fb.size());
    assertEquals(1, fb.get(0).blockIndex);
    assertEquals(DEFAULT_BLOCK_SIZE, fb.get(0).offset);
    assertEquals(DEFAULT_BLOCK_SIZE, fb.get(0).length());
    assertEquals(2, fb.get(1).blockIndex);
    assertEquals(DEFAULT_BLOCK_SIZE * 2, fb.get(1).offset);
    assertEquals(DEFAULT_BLOCK_SIZE, fb.get(1).length());

    fb = List.copyOf(S3FileBlock.of(summary, DEFAULT_BLOCK_SIZE * 4 + 1, DEFAULT_BLOCK_SIZE / 2 - 1));
    assertEquals(1, fb.size());
    assertEquals(4, fb.get(0).blockIndex);
    assertEquals(DEFAULT_BLOCK_SIZE * 4, fb.get(0).offset);
    assertEquals(DEFAULT_BLOCK_SIZE / 2, fb.get(0).length());
  }
}