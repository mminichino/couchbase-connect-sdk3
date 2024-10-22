package com.codelry.util.cbdb3;

public class BucketData {
  private String name;
  private String type;
  private int quota;
  private int replicas;
  private String eviction;
  private int ttl;
  private String storage;
  private String resolution;
  private String password;

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public int getQuota() {
    return quota;
  }

  public int getReplicas() {
    return replicas;
  }

  public String getEviction() {
    return eviction;
  }

  public int getTtl() {
    return ttl;
  }

  public String getStorage() {
    return storage;
  }

  public String getResolution() {
    return resolution;
  }

  public String getPassword() {
    return password;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setQuota(int quota) {
    this.quota = quota;
  }

  public void setReplicas(int replicas) {
    this.replicas = replicas;
  }

  public void setEviction(String eviction) {
    this.eviction = eviction;
  }

  public void setTtl(int ttl) {
    this.ttl = ttl;
  }

  public void setStorage(String storage) {
    this.storage = storage;
  }

  public void setResolution(String resolution) {
    this.resolution = resolution;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
