package com.codelry.util;

public class CollectionData {
  private String name;
  private int ttl;
  private boolean history;

  public String getName() {
    return name;
  }

  public int getTtl() {
    return ttl;
  }

  public boolean getHistory() {
    return history;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setTtl(int ttl) {
    this.ttl = ttl;
  }

  public void setHistory(boolean history) {
    this.history = history;
  }
}
