package com.codelry.util;

import com.fasterxml.jackson.databind.JsonNode;

public class SearchIndexData {
  private String bucket;
  private String scope;
  private String name;
  private String type;
  private JsonNode config;

  public String getBucket() {
    return bucket;
  }

  public String getName() {
    return name;
  }

  public String getScope() {
    return scope;
  }

  public String getType() {
    return type;
  }

  public JsonNode getConfig() {
    return config;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setScope(String scope) {
    this.scope = scope;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setConfig(JsonNode config) {
    this.config = config;
  }
}
