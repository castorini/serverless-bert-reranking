package io.anlessini;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.anlessini.store.S3BlockCache;
import io.anlessini.store.S3Directory;
import io.anlessini.store.S3IndexInput;
import io.anserini.index.IndexArgs;
import io.anserini.search.query.BagOfWordsQueryGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;

import java.io.*;
import java.util.*;

public class SearchLambda implements RequestHandler<SearchRequest, SearchResponse> {
  public static final Sort BREAK_SCORE_TIES_BY_DOCID =
      new Sort(SortField.FIELD_SCORE, new SortField(IndexArgs.ID, SortField.Type.STRING_VAL));
  private static final Logger LOG = LogManager.getLogger(SearchLambda.class);

  private final AmazonS3 s3Client;
  private final S3Directory directory;
  private final IndexReader reader;
  private final Analyzer analyzer;
  private static final String S3_INDEX_BUCKET = System.getenv("INDEX_BUCKET");
  private static final String S3_INDEX_KEY = System.getenv("INDEX_KEY");

  public SearchLambda() throws IOException {
    s3Client = AmazonS3ClientBuilder.defaultClient();
    directory = new S3Directory(s3Client, S3_INDEX_BUCKET, S3_INDEX_KEY);
    reader = DirectoryReader.open(directory);
    analyzer = new EnglishAnalyzer();
  }

  @Override
  public SearchResponse handleRequest(SearchRequest input, Context context) {
    try {
      LOG.info("Received input: " + input);
      long startTime = System.currentTimeMillis();
      Similarity similarity = new BM25Similarity(input.getBm25k1(), input.getBm25b());
      IndexSearcher searcher = new IndexSearcher(reader);
      searcher.setSimilarity(similarity);
      searcher.setQueryCache(null); // disable query caching

      Query query = new BagOfWordsQueryGenerator().buildQuery(IndexArgs.CONTENTS, analyzer, input.getQuery());
      TopDocs topDocs = searcher.search(query, input.getMaxDocs(), BREAK_SCORE_TIES_BY_DOCID, true);

      List<SearchResponse.Hit> hits = new ArrayList<>();
      for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
        Document doc = reader.document(scoreDoc.doc);
        hits.add(new SearchResponse.Hit(doc.get(IndexArgs.ID), scoreDoc.score, scoreDoc.doc));
      }
      SearchResponse response = new SearchResponse(hits);

      LOG.trace("Response: " + response);
      long endTime = System.currentTimeMillis();
      LOG.info("Query latency: " + (endTime - startTime) + " ms");

      S3BlockCache.getInstance().logStats();
      S3IndexInput.logStats();
      if (Boolean.parseBoolean(System.getenv("CLEAR_CACHE_STATS"))) {
        S3BlockCache.getInstance().clearStats();
        S3IndexInput.clearStats();
      }

      return response;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
