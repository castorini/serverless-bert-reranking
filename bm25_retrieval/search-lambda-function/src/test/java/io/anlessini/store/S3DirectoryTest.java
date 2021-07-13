package io.anlessini.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectResult;
import io.anserini.index.IndexArgs;
import io.anserini.search.query.BagOfWordsQueryGenerator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

public class S3DirectoryTest {
  public static final String TEST_BUCKET = "test-bucket";
  public static final String TEST_KEY = "test-key";

  @Rule
  public LocalStackContainer localstack = new LocalStackContainer()
      .withServices(S3);

  public AmazonS3 s3;

  @Before
  public void setUp() {
    s3 = AmazonS3ClientBuilder
        .standard()
        .withEndpointConfiguration(localstack.getEndpointConfiguration(S3))
        .withCredentials(localstack.getDefaultCredentialsProvider())
        .build();
    s3.createBucket(TEST_BUCKET);
  }

  @Test
  public void testReadS3Directory() throws Exception {
    Path path = Paths.get("src/test/resources/sample_index/trec/collection1/lucene-index.collection1.pos+docvectors+rawdocs+contents");
    File indexDir = new File(path.toString());
    File[] indexFiles = indexDir.listFiles();
    assertNotNull(indexFiles);
    for (File indexFile: indexFiles) {
      PutObjectResult result = s3.putObject(TEST_BUCKET, TEST_KEY + "/" + indexFile.getName(), indexFile);
    }

    S3Directory s3Directory = new S3Directory(s3, TEST_BUCKET, TEST_KEY);
    IndexReader reader = DirectoryReader.open(s3Directory);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setSimilarity(new BM25Similarity(0.9f, 0.4f));
    searcher.setQueryCache(null);

    assertEquals(2, reader.maxDoc());

    Analyzer analyzer = new EnglishAnalyzer();
    Query query = new BagOfWordsQueryGenerator().buildQuery(IndexArgs.CONTENTS, analyzer, "Hopefully we get this right");
    TopDocs hits = searcher.search(query, 1);
    assertEquals(1, hits.scoreDocs.length);
    assertEquals("AP-0001", searcher.doc(hits.scoreDocs[0].doc).get(IndexArgs.ID));

    query = new BagOfWordsQueryGenerator().buildQuery(IndexArgs.CONTENTS, analyzer, "right text");
    hits = searcher.search(query, 10);
    assertEquals(2, hits.scoreDocs.length);
  }
}
