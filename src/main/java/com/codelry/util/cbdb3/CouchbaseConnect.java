package com.codelry.util.cbdb3;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.manager.bucket.BucketType;
import com.couchbase.client.java.manager.bucket.ConflictResolutionType;
import com.couchbase.client.java.manager.bucket.StorageBackend;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Set;

import com.couchbase.client.java.manager.user.Role;

/**
 * Couchbase SDK 3.x connection utility.
 */
public interface CouchbaseConnect {

  static CouchbaseConnect getInstance() {
    return Server.getInstance();
  }

  static BucketType convertBucketType(String bucketType) {
    switch (bucketType.toLowerCase()) {
      case "ephemeral":
        return BucketType.EPHEMERAL;
      case "memcached":
        return BucketType.MEMCACHED;
      default:
        return BucketType.COUCHBASE;
    }
  }

  static StorageBackend convertStorageBackend(String storageBackend) {
    if (storageBackend.equalsIgnoreCase("magma")) {
      return StorageBackend.MAGMA;
    }
    return StorageBackend.COUCHSTORE;
  }

  static ConflictResolutionType convertConflictResolutionType(String conflictResolutionType) {
    switch (conflictResolutionType.toLowerCase()) {
      case "custom":
        return ConflictResolutionType.CUSTOM;
      case "timestamp":
      case "lww":
        return ConflictResolutionType.TIMESTAMP;
      default:
        return ConflictResolutionType.SEQUENCE_NUMBER;
    }
  }

  void connect(CouchbaseConfig config);

  void disconnect();

  CouchbaseStream stream(String bucketName);

  CouchbaseStream stream(String bucketName, String scopeName, String collectionName);

  String hostValue();

  String adminUserValue();

  String adminPasswordValue();

  String getBucketName();

  String getScopeName();

  String getCollectionName();

  Cluster getCluster();

  Bucket getBucket();

  Scope getScope();

  Collection getCollection();

  ReactiveCollection getReactiveCollection();

  String getKeyspace();

  long getIndexNodeCount();

  List<String> listBuckets();

  Boolean isBucket(String bucket);

  Boolean isBucket();

  void clusterWait();

  void clusterPing();

  void connectBucket(String name);

  void connectBucket();

  void connectScope(String name);

  void connectScope();

  void connectCollection(String name);

  void connectCollection(String scopeName, String collectionName);

  void connectCollection();

  void connectKeyspace(String bucketName, String scopeName, String collectionName);

  void connectKeyspace();

  void createBucket();

  void createBucket(String name);

  void createBucket(String name, int quota);

  void createBucket(String name, int quota, int replicas);

  void createBucket(String name, int quota, int replicas, String storageBackend);

  void createBucket(String name, int quota, int replicas, String bucketType, String storageBackend);

  void createBucket(String name, int quota, int replicas, BucketType bucketType, StorageBackend storageBackend);

  void createBucket(BucketData bucketData);

  void bucketCreate(String name, int quota, int replicas, BucketType bucketType, StorageBackend storageBackend);

  void dropBucket(String name);

  void dropBucket();

  void createScope(String bucketName, String scopeName);

  void createScope();

  void createCollection(String bucketName, String scopeName, String collectionName);

  void createCollection();

  boolean collectionExists(String bucketName, String scopeName, String collectionName);

  void createPrimaryIndex();

  void createPrimaryIndex(String bucketName, String scopeName, String collectionName);

  void createPrimaryIndex(String bucketName, String scopeName, String collectionName, int replicaCount);

  void createSecondaryIndex(String indexName, List<String> indexKeys);

  void createSecondaryIndex(String bucketName, String scopeName, String collectionName, String indexName, List<String> indexKeys);

  void createSecondaryIndex(String bucketName, String scopeName, String collectionName, String indexName, List<String> indexKeys, int replicaCount);

  void createSearchIndex(JsonNode config);

  Set<Role> defaultRoles();

  Set<Role> constructRoles(List<RoleData> roles);

  void createUser(String userName, String passWord, String fullName, List<String> groups, List<RoleData> roles);

  void createGroup(String groupName, String description, List<RoleData> roles);

  JsonNode get(String id);

  void upsert(String id, Object content);

  List<JsonNode> query(String queryString);

  List<String> getStringList(JsonNode node);

  List<SearchIndexData> getSearchIndexes(String bucket, String scope);

  List<IndexData> getIndexes(String bucket, String collection);

  List<TableData> getBuckets();

  RoleData parseRole(JsonNode role);

  List<UserData> getUsers();

  List<GroupData> getGroups();

  void createBuckets(List<TableData> buckets);
}
