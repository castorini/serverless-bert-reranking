package io.anlessini;

import java.io.Serializable;
import java.util.List;

public class SearchResponse implements Serializable, Cloneable {
  public final List<Hit> hits;

  public SearchResponse(List<Hit> hits) {
    this.hits = hits;
  }

  public static class Hit implements Serializable, Cloneable {
    /** The document identifier field
     *  @see io.anserini.index.IndexArgs#ID */
    public final String docid;

    /** The score of this document for the query */
    public final Float score;

    /** The document's number
     *  @see org.apache.lucene.search.ScoreDoc#doc */
    public final Integer doc;

    public Hit(String docid, Float score, Integer doc) {
      this.docid = docid;
      this.score = score;
      this.doc = doc;
    }

    @Override
    public String toString() {
      return "Hit{" +
          "docid='" + docid + '\'' +
          ", score=" + score +
          ", doc=" + doc +
          '}';
    }
  }

  @Override
  public String toString() {
    return "SearchResponse{" +
        "hits=" + hits +
        '}';
  }
}
