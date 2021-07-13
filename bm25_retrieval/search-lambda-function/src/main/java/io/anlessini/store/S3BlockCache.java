package io.anlessini.store;

import com.google.common.util.concurrent.AtomicLongMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class S3BlockCache {
  private static final Logger LOG = LogManager.getLogger(S3BlockCache.class);

  static final int DEFAULT_INITIAL_CACHE_SIZE = 16;
  static final float DEFAULT_LOAD_FACTOR = 0.75f;
  static final int DEFAULT_CONCURRENCY_LEVEL = 16;
  /**
   * A block from a file is evictable if the file is at least 32 MB
   */
  static final int MIN_EVICTABLE_SIZE = 1024 * 1024 * 32;
  /**
   * The eviction threshold, if we exceed 1792 MB in heap size we run eviction
   */
  static final long MAX_HEAP_SIZE = 1024 * 1024 * 1792;

  private final Map<S3FileBlock, CacheBlob> cache;
  /**
   * Current size of cache in bytes
   */
  private final AtomicLong size = new AtomicLong();
  /**
   * Cache access count (sequential ID)
   */
  private final AtomicLong count = new AtomicLong();
  /**
   * Current number of cached elements
   */
  private final AtomicLong elements = new AtomicLong();
  /**
   * The number of cache hits
   */
  private final AtomicLongMap<String> hitCount = AtomicLongMap.create();
  /**
   * The number of cache misses
   */
  private final AtomicLongMap<String> missCount = AtomicLongMap.create();
  /**
   * The number of cache block eviction
   */
  private final AtomicLongMap<String> evictCount = AtomicLongMap.create();

  private volatile boolean evictionInProgress = false;
  private final ReentrantLock evictionLock = new ReentrantLock(true);

  private final Comparator<CacheBlob> lruCacheBlockComparator = Comparator.comparingLong(CacheBlob::getAccessTime);

  private static S3BlockCache instance;

  public static synchronized S3BlockCache getInstance() {
    if (instance == null) {
      instance = new S3BlockCache();
    }
    return instance;
  }

  private S3BlockCache() {
    cache = new ConcurrentHashMap<>(DEFAULT_INITIAL_CACHE_SIZE, DEFAULT_LOAD_FACTOR, DEFAULT_CONCURRENCY_LEVEL);
  }

  protected void cacheBlock(S3FileBlock fileBlock, byte[] data) {
    CacheBlob cb = cache.get(fileBlock);
    if (cb != null) {
      LOG.warn("Cache block already in memory: " + fileBlock);
      return;
    }
    long accessTime = count.incrementAndGet();
    cb = new CacheBlob(fileBlock, data, accessTime);
    long newSize = size.addAndGet(cb.size());
    cache.put(fileBlock, cb);
    elements.incrementAndGet();
    LOG.trace("Cached block " + fileBlock + " with " + data.length + " bytes at " + accessTime);
    if (newSize > MAX_HEAP_SIZE && !evictionInProgress) {
      evict();
    }
  }

  protected void evict() {
    if (!evictionLock.tryLock()) return;

    try {
      evictionInProgress = true;
      long currentSize = size.get();
      long bytesToFree = currentSize - (long) (MAX_HEAP_SIZE * 0.75f);

      PriorityQueue<CacheBlob> evictionQueue = new PriorityQueue<>(lruCacheBlockComparator);
      for (CacheBlob cb : cache.values()) {
        evictionQueue.offer(cb);
      }
      long freedBytes = 0;
      CacheBlob cb;
      while ((cb = evictionQueue.poll()) != null) {
        if (cb.size() < MIN_EVICTABLE_SIZE) continue;
        LOG.trace("Evicted block " + cb.fileBlock + " with " + cb.size() + " bytes");
        cache.remove(cb.fileBlock);
        evictCount.incrementAndGet(cb.fileBlock.summary.getKey());
        size.addAndGet(-1 * cb.size());
        freedBytes += cb.size();
        if (freedBytes >= bytesToFree) {
          break;
        }
      }
    } finally {
      evictionInProgress = false;
      evictionLock.unlock();
    }
  }

  protected byte[] getBlock(S3FileBlock fileBlock) {
    CacheBlob cb = cache.get(fileBlock);
    long accessTime = count.incrementAndGet();
    if (cb == null) {
      LOG.trace("Missed block " + fileBlock + " at " + accessTime);
      missCount.incrementAndGet(fileBlock.summary.getKey());
      return null;
    }
    LOG.trace("Accessed block " + fileBlock + " at " + accessTime);
    hitCount.incrementAndGet(fileBlock.summary.getKey());
    cb.access(accessTime);
    return cb.data;
  }

  public void logStats() {
    if (!LOG.isTraceEnabled()) return;

    Set<String> fileKeys = new HashSet<>();
    fileKeys.addAll(hitCount.asMap().keySet());
    fileKeys.addAll(missCount.asMap().keySet());
    fileKeys.addAll(evictCount.asMap().keySet());

    LOG.trace("================================= Cache Stats =================================");
    LOG.trace(String.format("%-20s %10s %10s %10s", "Key", "Hits", "Misses", "Evictions"));
    fileKeys.stream().sorted().forEach(key -> {
      LOG.trace(String.format("%-20s %,10d %,10d %,10d", key, hitCount.get(key), missCount.get(key), evictCount.get(key)));
    });
    LOG.trace("Total cache size=" + size.get() + ", elements=" + elements.get());
  }

  public void clearStats() {
    hitCount.clear();
    missCount.clear();
    evictCount.clear();
  }

  public static class CacheBlob {
    public final S3FileBlock fileBlock;
    public final byte[] data;
    private volatile long accessTime;

    public CacheBlob(S3FileBlock fileBlock, byte[] data, long accessTime) {
      this.fileBlock = fileBlock;
      this.data = data;
      this.accessTime = accessTime;
    }

    public int size() {
      return data.length;
    }

    public void access(long accessTime) {
      LOG.trace("Accessed block " + fileBlock + " at " + accessTime);
      this.accessTime = accessTime;
    }

    public long getAccessTime() {
      return accessTime;
    }
  }
}
