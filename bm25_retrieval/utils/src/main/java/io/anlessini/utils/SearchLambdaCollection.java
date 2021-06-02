package io.anlessini.utils;

import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
import com.google.gson.Gson;
import io.anlessini.SearchRequest;
import io.anlessini.SearchResponse;
import io.anserini.search.topicreader.TopicReader;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.*;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SearchLambdaCollection<K> implements Closeable {
  private static final Logger LOG = LogManager.getLogger(SearchLambdaCollection.class);

  public static class Args {
    // required arguments
    @Option(name = "-lambda", required = true, usage = "The ARN of the search lambda.")
    public String lambda;

    @Option(name = "-topics", metaVar = "[file]", handler = StringArrayOptionHandler.class, required = true, usage = "topics file")
    public String[] topics;

    @Option(name = "-topic.reader", required = true, usage = "TopicReader to use.")
    public String topicReader;

    @Option(name = "-output", metaVar = "[file]", required = true, usage = "Output run file.")
    public String output;

    @Option(name = "-threads", metaVar = "[Number]", usage = "Number of Threads")
    public int threads = 8;

    @Option(name = "-topic.fields", handler = StringArrayOptionHandler.class, usage = "Which field of the query should be used, default \"title\"." +
        " For TREC ad hoc topics, description or narrative can be used.")
    public String[] topicFields = new String[]{"title"};

    @Option(name = "-hits", metaVar = "[number]", usage = "max number of hits to return")
    public int hits = 1000;

    @Option(name = "-bm25.k1", metaVar = "[number]", usage = "BM25: k1 parameter")
    public float bm25k1 = 0.9f;

    @Option(name = "-bm25.b", metaVar = "[number]", usage = "BM25: b parameter")
    public float bm25b = 0.4f;

    @Option(name = "-remove.duplicates", usage = "Remove duplicate docids when writing final run output.")
    public Boolean removeDuplicates = false;

    @Option(name = "-strip.segment.id", usage = "Remove the .XXXXX suffix used to denote different segments from an document")
    public Boolean stripSegmentId = false;

    @Option(name = "-report.interval", metaVar = "[number]", usage = "The number of queries processed in between report log messages.")
    public int reportInterval = 200;

    @Option(name = "-runtag", metaVar = "[tag]", usage = "runtag")
    public String runtag = "anlessini";
  }

  private final Args args;
  private final AWSLambda lambda; // thread-safe AWS lambda client
  private final SortedMap<K, Map<String, String>> topics;
  private final Gson gson = new Gson();
  private final AtomicLong processedQueries = new AtomicLong();
  private final PrintWriter out;

  @SuppressWarnings("unchecked")
  public SearchLambdaCollection(Args args) throws IOException {
    this.args = args;
    this.lambda = AWSLambdaClientBuilder.defaultClient();
    this.topics = new TreeMap<>();
    this.out = new PrintWriter(Files.newBufferedWriter(Paths.get(args.output), StandardCharsets.US_ASCII));

    for (String topicsFile : args.topics) {
      Path path = Paths.get(topicsFile);
      if (!Files.exists(path) || !Files.isRegularFile(path) || !Files.isReadable(path)) {
        throw new IllegalArgumentException("Topics file " + path + " does not exist or is not a (readable) file.");
      }
      try {
        TopicReader<K> tr = (TopicReader<K>) Class.forName("io.anserini.search.topicreader." + args.topicReader + "TopicReader")
            .getConstructor(Path.class).newInstance(path);
        this.topics.putAll(tr.read());
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to load topic " + path + " using topic reader " + args.topicReader, e);
      }
    }
  }

  public void runTopics() {
    final long start = System.nanoTime();
    final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(args.threads);

    for (Map.Entry<K, Map<String, String>> topicEntry : topics.entrySet()) {
      final K qid = topicEntry.getKey();
      final Map<String, String> fieldValues = topicEntry.getValue();
      executor.execute(() -> {
        StringBuilder sb = new StringBuilder();
        for (String field : args.topicFields) {
          sb.append(" ").append(fieldValues.get(field.trim()));
        }
        String queryString = sb.toString();

        SearchRequest request = new SearchRequest(queryString, args.hits, args.bm25k1, args.bm25b);
        InvokeRequest invokeRequest = new InvokeRequest()
            .withFunctionName(args.lambda)
            .withInvocationType(InvocationType.RequestResponse)
            .withPayload(gson.toJson(request));

        InvokeResult invokeResult = lambda.invoke(invokeRequest);
        String payload = StandardCharsets.UTF_8.decode(invokeResult.getPayload().asReadOnlyBuffer()).toString();

        if (invokeResult.getStatusCode() != 200 || invokeResult.getFunctionError() != null) {
          String logMessage = invokeResult.getLogResult() != null ? new String(Base64.getDecoder().decode(invokeResult.getLogResult())) : "";
          throw new RuntimeException("Invocation " + request + " failed with code=" + invokeResult.getStatusCode() +
              "\nerror=" + invokeResult.getFunctionError() + "\npayload=" + payload + "\nlogMessage=" + logMessage);
        }

        SearchResponse response = gson.fromJson(payload, SearchResponse.class);

        Set<String> docids = new HashSet<>();
        int rank = 1;
        StringBuilder buf = new StringBuilder();
        for (SearchResponse.Hit hit : response.hits) {
          String docid = hit.docid;
          if (args.stripSegmentId) {
            docid = docid.split("\\.")[0];
          }

          if (args.removeDuplicates) {
            if (docids.contains(docid)) {
              continue;
            } else {
              docids.add(docid);
            }
          }

          buf.append(String.format(Locale.US, "%s Q0 %s %d %f %s\n",
              qid, docid, rank, hit.score, args.runtag));

          rank++;
        }
        out.println(buf.toString());
        long processed = processedQueries.incrementAndGet();
        if (processed % args.reportInterval == 0) {
          LOG.info(String.format("%d queries processed", processed));
        }
      });
    }

    executor.shutdown();

    try {
      // Wait for existing tasks to terminate
      while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
        LOG.info(String.format("%d queries processed", processedQueries.get()));
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      executor.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }

    out.flush();
    final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    LOG.info(processedQueries.get() + " topics processed in " + DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));
  }

  @Override
  public void close() {
    out.close();
  }

  public static void main(String[] args) throws Exception {
    Args searchArgs = new Args();
    CmdLineParser parser = new CmdLineParser(searchArgs, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.err.println("Example: SearchLambdaCollection" + parser.printExample(OptionHandlerFilter.REQUIRED));
      return;
    }

    final long start = System.nanoTime();
    SearchLambdaCollection searcher = new SearchLambdaCollection(searchArgs);
    searcher.runTopics();
    searcher.close();
    final long durationMillis = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    LOG.info("Total run time: " + DurationFormatUtils.formatDuration(durationMillis, "HH:mm:ss"));
  }
}
