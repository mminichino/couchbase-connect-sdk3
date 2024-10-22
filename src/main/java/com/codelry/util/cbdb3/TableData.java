package com.codelry.util.cbdb3;

import java.util.List;

public class TableData {
  private String name;
  private TableType type;
  private BucketData bucket;
  private String password;
  private ScopeData scope;
  private CollectionData collection;
  private List<IndexData> indexes;
  private List<SearchIndexData> searchIndexes;

  public String getName() {
    return name;
  }

  public TableType getType() {
    return type;
  }

  public BucketData getBucket() {
    return bucket;
  }

  public String getPassword() {
    return password;
  }

  public ScopeData getScope() {
    return scope;
  }

  public CollectionData getCollection() {
    return collection;
  }

  public List<IndexData> getIndexes() {
    return indexes;
  }

  public List<SearchIndexData> getSearchIndexes() {
    return searchIndexes;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setType(TableType type) {
    this.type = type;
  }

  public void setBucket(BucketData bucket) {
    this.bucket = bucket;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setScope(ScopeData scope) {
    this.scope = scope;
  }

  public void setCollection(CollectionData collection) {
    this.collection = collection;
  }

  public void setIndexes(List<IndexData> indexes) {
    this.indexes = indexes;
  }

  public void setSearchIndexes(List<SearchIndexData> searchIndexes) {
    this.searchIndexes = searchIndexes;
  }

  public static TableData inList(List<TableData> tables, String name) {
    for (TableData t : tables) {
      if (t.getName().equals(name)) {
        return t;
      }
    }
    return null;
  }
}
