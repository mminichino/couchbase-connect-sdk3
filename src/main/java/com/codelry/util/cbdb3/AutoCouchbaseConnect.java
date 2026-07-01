package com.codelry.util.cbdb3;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.manager.bucket.BucketType;
import com.couchbase.client.java.manager.bucket.StorageBackend;
import com.couchbase.client.java.manager.user.Role;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Routes {@link CouchbaseConnect} calls to {@link Capella} or {@link Server}
 * based on the supplied {@link CouchbaseConfig}.
 */
final class AutoCouchbaseConnect implements CouchbaseConnect {
  private static final AutoCouchbaseConnect INSTANCE = new AutoCouchbaseConnect();
  private volatile CouchbaseConnect delegate;

  private AutoCouchbaseConnect() {}

  static AutoCouchbaseConnect getInstance() {
    return INSTANCE;
  }

  private CouchbaseConnect requireDelegate() {
    CouchbaseConnect current = delegate;
    if (current == null) {
      throw new IllegalStateException("Call connect(CouchbaseConfig) or createCluster(CouchbaseConfig) first");
    }
    return current;
  }

  private void assignDelegate(CouchbaseConfig config) {
    delegate = CouchbaseConnect.resolve(config);
  }

  @Override
  public void connect(CouchbaseConfig config) {
    assignDelegate(config);
    delegate.connect(config);
  }

  @Override
  public void createCluster(CouchbaseConfig config) {
    assignDelegate(config);
    delegate.createCluster(config);
  }

  @Override
  public void createCluster(CouchbaseConfig config, Map<String, String> options) {
    assignDelegate(config);
    delegate.createCluster(config, options);
  }

  @Override
  public void destroyCluster() {
    requireDelegate().destroyCluster();
  }

  @Override
  public void disconnect() {
    requireDelegate().disconnect();
  }

  @Override
  public CouchbaseStream stream(String bucketName) {
    return requireDelegate().stream(bucketName);
  }

  @Override
  public CouchbaseStream stream(String bucketName, String scopeName, String collectionName) {
    return requireDelegate().stream(bucketName, scopeName, collectionName);
  }

  @Override
  public String hostValue() {
    return requireDelegate().hostValue();
  }

  @Override
  public String adminUserValue() {
    return requireDelegate().adminUserValue();
  }

  @Override
  public String adminPasswordValue() {
    return requireDelegate().adminPasswordValue();
  }

  @Override
  public String getBucketName() {
    return requireDelegate().getBucketName();
  }

  @Override
  public String getScopeName() {
    return requireDelegate().getScopeName();
  }

  @Override
  public String getCollectionName() {
    return requireDelegate().getCollectionName();
  }

  @Override
  public Cluster getCluster() {
    return requireDelegate().getCluster();
  }

  @Override
  public Bucket getBucket() {
    return requireDelegate().getBucket();
  }

  @Override
  public Scope getScope() {
    return requireDelegate().getScope();
  }

  @Override
  public Collection getCollection() {
    return requireDelegate().getCollection();
  }

  @Override
  public ReactiveCollection getReactiveCollection() {
    return requireDelegate().getReactiveCollection();
  }

  @Override
  public String getKeyspace() {
    return requireDelegate().getKeyspace();
  }

  @Override
  public long getIndexNodeCount() {
    return requireDelegate().getIndexNodeCount();
  }

  @Override
  public List<String> listBuckets() {
    return requireDelegate().listBuckets();
  }

  @Override
  public Boolean isBucket(String bucket) {
    return requireDelegate().isBucket(bucket);
  }

  @Override
  public Boolean isBucket() {
    return requireDelegate().isBucket();
  }

  @Override
  public void clusterWait() {
    requireDelegate().clusterWait();
  }

  @Override
  public void clusterPing() {
    requireDelegate().clusterPing();
  }

  @Override
  public void connectBucket(String name) {
    requireDelegate().connectBucket(name);
  }

  @Override
  public void connectBucket() {
    requireDelegate().connectBucket();
  }

  @Override
  public void connectScope(String name) {
    requireDelegate().connectScope(name);
  }

  @Override
  public void connectScope() {
    requireDelegate().connectScope();
  }

  @Override
  public void connectCollection(String name) {
    requireDelegate().connectCollection(name);
  }

  @Override
  public void connectCollection(String scopeName, String collectionName) {
    requireDelegate().connectCollection(scopeName, collectionName);
  }

  @Override
  public void connectCollection() {
    requireDelegate().connectCollection();
  }

  @Override
  public void connectKeyspace(String bucketName, String scopeName, String collectionName) {
    requireDelegate().connectKeyspace(bucketName, scopeName, collectionName);
  }

  @Override
  public void connectKeyspace() {
    requireDelegate().connectKeyspace();
  }

  @Override
  public void createBucket() {
    requireDelegate().createBucket();
  }

  @Override
  public void createBucket(String name) {
    requireDelegate().createBucket(name);
  }

  @Override
  public void createBucket(String name, int quota) {
    requireDelegate().createBucket(name, quota);
  }

  @Override
  public void createBucket(String name, int quota, int replicas) {
    requireDelegate().createBucket(name, quota, replicas);
  }

  @Override
  public void createBucket(String name, int quota, int replicas, String storageBackend) {
    requireDelegate().createBucket(name, quota, replicas, storageBackend);
  }

  @Override
  public void createBucket(String name, int quota, int replicas, String bucketType, String storageBackend) {
    requireDelegate().createBucket(name, quota, replicas, bucketType, storageBackend);
  }

  @Override
  public void createBucket(String name, int quota, int replicas, BucketType bucketType, StorageBackend storageBackend) {
    requireDelegate().createBucket(name, quota, replicas, bucketType, storageBackend);
  }

  @Override
  public void createBucket(BucketData bucketData) {
    requireDelegate().createBucket(bucketData);
  }

  @Override
  public void bucketCreate(String name, int quota, int replicas, BucketType bucketType, StorageBackend storageBackend) {
    requireDelegate().bucketCreate(name, quota, replicas, bucketType, storageBackend);
  }

  @Override
  public void dropBucket(String name) {
    requireDelegate().dropBucket(name);
  }

  @Override
  public void dropBucket() {
    requireDelegate().dropBucket();
  }

  @Override
  public void createScope(String bucketName, String scopeName) {
    requireDelegate().createScope(bucketName, scopeName);
  }

  @Override
  public void createScope() {
    requireDelegate().createScope();
  }

  @Override
  public void createCollection(String bucketName, String scopeName, String collectionName) {
    requireDelegate().createCollection(bucketName, scopeName, collectionName);
  }

  @Override
  public void createCollection() {
    requireDelegate().createCollection();
  }

  @Override
  public boolean collectionExists(String bucketName, String scopeName, String collectionName) {
    return requireDelegate().collectionExists(bucketName, scopeName, collectionName);
  }

  @Override
  public void createPrimaryIndex() {
    requireDelegate().createPrimaryIndex();
  }

  @Override
  public void createPrimaryIndex(String bucketName, String scopeName, String collectionName) {
    requireDelegate().createPrimaryIndex(bucketName, scopeName, collectionName);
  }

  @Override
  public void createPrimaryIndex(String bucketName, String scopeName, String collectionName, int replicaCount) {
    requireDelegate().createPrimaryIndex(bucketName, scopeName, collectionName, replicaCount);
  }

  @Override
  public void createSecondaryIndex(String indexName, List<String> indexKeys) {
    requireDelegate().createSecondaryIndex(indexName, indexKeys);
  }

  @Override
  public void createSecondaryIndex(String bucketName, String scopeName, String collectionName, String indexName,
      List<String> indexKeys) {
    requireDelegate().createSecondaryIndex(bucketName, scopeName, collectionName, indexName, indexKeys);
  }

  @Override
  public void createSecondaryIndex(String bucketName, String scopeName, String collectionName, String indexName,
      List<String> indexKeys, int replicaCount) {
    requireDelegate().createSecondaryIndex(bucketName, scopeName, collectionName, indexName, indexKeys, replicaCount);
  }

  @Override
  public void createSearchIndex(JsonNode config) {
    requireDelegate().createSearchIndex(config);
  }

  @Override
  public Set<Role> defaultRoles() {
    return requireDelegate().defaultRoles();
  }

  @Override
  public Set<Role> constructRoles(List<RoleData> roles) {
    return requireDelegate().constructRoles(roles);
  }

  @Override
  public void createUser(String userName, String passWord, String fullName, List<String> groups, List<RoleData> roles) {
    requireDelegate().createUser(userName, passWord, fullName, groups, roles);
  }

  @Override
  public void createGroup(String groupName, String description, List<RoleData> roles) {
    requireDelegate().createGroup(groupName, description, roles);
  }

  @Override
  public JsonNode get(String id) {
    return requireDelegate().get(id);
  }

  @Override
  public void upsert(String id, Object content) {
    requireDelegate().upsert(id, content);
  }

  @Override
  public List<JsonNode> query(String queryString) {
    return requireDelegate().query(queryString);
  }

  @Override
  public List<String> getStringList(JsonNode node) {
    return requireDelegate().getStringList(node);
  }

  @Override
  public List<SearchIndexData> getSearchIndexes(String bucket, String scope) {
    return requireDelegate().getSearchIndexes(bucket, scope);
  }

  @Override
  public List<IndexData> getIndexes(String bucket, String collection) {
    return requireDelegate().getIndexes(bucket, collection);
  }

  @Override
  public List<TableData> getBuckets() {
    return requireDelegate().getBuckets();
  }

  @Override
  public RoleData parseRole(JsonNode role) {
    return requireDelegate().parseRole(role);
  }

  @Override
  public List<UserData> getUsers() {
    return requireDelegate().getUsers();
  }

  @Override
  public List<GroupData> getGroups() {
    return requireDelegate().getGroups();
  }

  @Override
  public void createBuckets(List<TableData> buckets) {
    requireDelegate().createBuckets(buckets);
  }
}
