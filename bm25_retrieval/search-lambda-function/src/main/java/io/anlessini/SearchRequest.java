package io.anlessini;

import java.io.Serializable;

public class SearchRequest implements Serializable, Cloneable {
  static final Integer DEFAULT_MAX_DOCS = 10;
  static final Float DEFAULT_BM25_K1 = 0.9f;
  static final Float DEFAULT_BM25_B = 0.4f;

  private String query;
  private Integer maxDocs;
  private Float bm25k1;
  private Float bm25b;

  public SearchRequest() {
    setMaxDocs(DEFAULT_MAX_DOCS);
    setBm25k1(DEFAULT_BM25_K1);
    setBm25b(DEFAULT_BM25_B);
  }

  public SearchRequest(String query, Integer maxDocs, Float bm25k1, Float bm25b) {
    this.query = query;
    this.maxDocs = maxDocs;
    this.bm25k1 = bm25k1;
    this.bm25b = bm25b;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public Integer getMaxDocs() {
    return maxDocs;
  }

  public void setMaxDocs(Integer maxDocs) {
    this.maxDocs = maxDocs;
  }

  public Float getBm25k1() {
    return bm25k1;
  }

  public void setBm25k1(Float bm25k1) {
    this.bm25k1 = bm25k1;
  }

  public Float getBm25b() {
    return bm25b;
  }

  public void setBm25b(Float bm25b) {
    this.bm25b = bm25b;
  }

  @Override
  public String toString() {
    return "SearchRequest{" +
        "query='" + query + '\'' +
        ", maxDocs=" + maxDocs +
        ", bm25k1=" + bm25k1 +
        ", bm25b=" + bm25b +
        '}';
  }
}
