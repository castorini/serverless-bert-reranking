package io.anlessini;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.anlessini.store.S3Directory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.FSDirectory;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

// DynamoDB stuff
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

public class SearchDemo {
  public static Query buildQuery(String field, Analyzer analyzer, String queryText) {
    List<String> tokens = tokenize(analyzer, queryText);

    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (String t : tokens) {
      builder.add(new TermQuery(new Term(field, t)), BooleanClause.Occur.SHOULD);
    }

    return builder.build();
  }

  static public List<String> tokenize(Analyzer analyzer, String s) {
    List<String> list = new ArrayList<>();

    try {
      TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(s));
      CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
      tokenStream.reset();
      while (tokenStream.incrementToken()) {
        if (cattr.toString().length() == 0) {
          continue;
        }
        list.add(cattr.toString());
      }
      tokenStream.end();
      tokenStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return list;
  }

  public static final class Args {
    @Option(name = "-bucket", metaVar = "[bucket]", usage = "S3 bucket")
    public String bucket = null;

    @Option(name = "-key", metaVar = "[key]", usage = "S3 key")
    public String key = null;

    @Option(name = "-index", metaVar = "[index]", usage = "local index")
    public String index = "acl";

    @Option(name = "-query", metaVar = "[query]", usage = "query")
    public String query = "How is the weather";

    @Option(name = "-verbose", usage = "verbose")
    public boolean verbose = false;

    @Option(name = "-cacheThreshold", metaVar = "[bytes]", usage = "cache files smaller than this size")
    public int cacheThreshold = 1024*1024*256;

    @Option(name = "-trials", metaVar = "[number]", usage = "number of repeated trials")
    public int trials = 0;
  }

  public static void main(String[] args) throws IOException {
    Args searchArgs = new Args();
    CmdLineParser parser = new CmdLineParser(searchArgs, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return;
    }

    if (!((searchArgs.bucket != null && searchArgs.key != null) ^ searchArgs.index != null)) {
      System.err.println("Must specify either a local index or a bucket/key combination!");
      return;
    }

    if (searchArgs.verbose) {
      Configurator.setLevel(S3Directory.class.getName(), Level.TRACE);
    }

    long startTime = System.currentTimeMillis();
    IndexReader reader;
    if (searchArgs.index != null ) {
      System.out.println("Searching local index...");
      reader = DirectoryReader.open(FSDirectory.open(Paths.get(searchArgs.index)));
    } else {
      System.out.println("Searching S3 index...");
      AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
      S3Directory directory = new S3Directory(s3Client, searchArgs.bucket, searchArgs.key);
      reader = DirectoryReader.open(directory);
    }

    Analyzer analyzer = new EnglishAnalyzer();
    Similarity similarity = new BM25Similarity(0.9f, 0.4f);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setSimilarity(similarity);
    searcher.setQueryCache(null);     // disable query caching

    System.out.println("Query: " + searchArgs.query);
    Query query = buildQuery("contents", analyzer, searchArgs.query);
    TopDocs topDocs = searcher.search(query, 10);
    System.out.println("Number of hits: " + topDocs.scoreDocs.length);
    String[] docids = new String[topDocs.scoreDocs.length];
    for (int i=0; i<topDocs.scoreDocs.length; i++) {
      Document doc = reader.document(topDocs.scoreDocs[i].doc);
      String docid = doc.getField("id").stringValue();
      docids[i] = docid;
    }

    for (int i=0; i<topDocs.scoreDocs.length; i++) {
      System.out.println(docids[i] + " " + topDocs.scoreDocs[i].score);
    }
    long endTime = System.currentTimeMillis();
    System.out.println("Query latency: " + (endTime-startTime) + " ms");

    long sum = 0;
    for (int i=0; i<searchArgs.trials; i++) {
      startTime = System.currentTimeMillis();
      query = buildQuery("contents", analyzer, searchArgs.query);
      topDocs = searcher.search(query, 10);
      endTime = System.currentTimeMillis();
      System.out.println("[trial " + i + "] Query latency: " + (endTime-startTime) + " ms");
      sum += (endTime-startTime);
    }
    if (searchArgs.trials > 0) {
      System.out.println("Average: " + sum / searchArgs.trials + " ms");
    }

    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    DynamoDB dynamoDB = new DynamoDB(client);
    Table table = dynamoDB.getTable("ACL");

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode rootNode = mapper.createObjectNode();
    rootNode.put("query_id", UUID.randomUUID().toString());
    ArrayNode response = mapper.createArrayNode();
    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
      // childNode.put("score", topDocs.scoreDocs[i].score);

      // retreiving from dynamodb
      Item item = table.getItem("id", docids[i]);
      response.add(item.toJSON());
    }
    rootNode.set("response", response);
    System.out.println(mapper.writeValueAsString(rootNode));

  }
}
