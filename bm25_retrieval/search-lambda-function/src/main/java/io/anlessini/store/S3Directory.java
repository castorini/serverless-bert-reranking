package io.anlessini.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class S3Directory extends BaseDirectory {
  private static final Logger LOG = LogManager.getLogger(S3Directory.class);

  private final AmazonS3 s3Client;

  private Map<String, S3ObjectSummary> objectSummaries;

  private final String bucket;
  private final String key;

  private final Lock lsLock = new ReentrantLock();

  public S3Directory(AmazonS3 s3Client, String bucket, String key) {
    super(new SingleInstanceLockFactory());
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.key = key;

    LOG.info("Opened S3Directory under " + bucket + "/" + key);
  }

  @Override
  public String[] listAll() {
    lsLock.lock();
    if (objectSummaries == null) { // only ls if has not already done so, otherwise use cached result
      objectSummaries = new HashMap<>();
      String listingCursor = null;
      ListObjectsV2Result result;
      do {
        ListObjectsV2Request req = new ListObjectsV2Request()
            .withBucketName(bucket)
            .withPrefix(key + "/")
            .withStartAfter(listingCursor);
        result = s3Client.listObjectsV2(req);
        List<S3ObjectSummary> listings = result.getObjectSummaries();
        for (S3ObjectSummary objectSummary: listings) {
            String objectKey = objectSummary.getKey();
            int keyLength = objectKey.split("/").length;
            String objectName;
            if (keyLength == 1){
                    objectName = ""; // we are reading something like "[objectKey]/", .split() treats it as array of size 1
            }
            else{
                    objectName = objectKey.split("/")[1];
            }
            objectSummaries.put(objectName, objectSummary);
        }
        listingCursor = listings.get(listings.size() - 1).getKey();
      } while (result.isTruncated());
    }
    lsLock.unlock();

    String[] result = objectSummaries.keySet().toArray(new String[objectSummaries.size()]);
    Arrays.sort(result);
    return result;
  }
@Override
  public void deleteFile(String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long fileLength(String name) {
    return objectSummaries.get(name).getSize();
  }

  @Override
  public Set<String> getPendingDeletions() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexOutput createOutput(String s, IOContext ioContext) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexOutput createTempOutput(String s, String s1, IOContext ioContext) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void sync(Collection<String> collection) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void syncMetaData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rename(String s, String s1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    return new S3IndexInput(s3Client, objectSummaries.get(name));
  }

  @Override
  public void close() {
    s3Client.shutdown();
  }
}