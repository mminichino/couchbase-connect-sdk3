package com.codelry.util;

public class RoleData {
  private String role;
  private String bucketName;
  private String scopeName;
  private String collectionName;

  public String getRole() {
    return role;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getScopeName() {
    return scopeName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public void setRole(String role) {
    this.role = role;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setScopeName(String scopeName) {
    this.scopeName = scopeName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }
}
