package com.codelry.util.cbdb3;

import com.couchbase.client.java.manager.bucket.BucketType;
import com.couchbase.client.java.manager.bucket.StorageBackend;
import java.util.Properties;
import static com.codelry.util.cbdb3.CouchbaseConnect.convertBucketType;
import static com.codelry.util.cbdb3.CouchbaseConnect.convertStorageBackend;

public class CouchbaseConfig {
  public static final String COUCHBASE_HOST = "couchbase.hostname";
  public static final String COUCHBASE_USER = "couchbase.username";
  public static final String COUCHBASE_PASSWORD = "couchbase.password";
  public static final String COUCHBASE_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SCOPE = "couchbase.scope";
  public static final String COUCHBASE_COLLECTION = "couchbase.collection";
  public static final String COUCHBASE_CLIENT_CERTIFICATE = "couchbase.client.cert";
  public static final String COUCHBASE_ROOT_CERTIFICATE = "couchbase.ca.cert";
  public static final String COUCHBASE_KEYSTORE_TYPE = "couchbase.keystore.type";
  public static final String COUCHBASE_SSL_MODE = "couchbase.sslMode";
  public static final String COUCHBASE_REPLICA_NUM = "couchbase.replicaNum";
  public static final String COUCHBASE_TTL = "couchbase.ttlSeconds";
  public static final String COUCHBASE_MAX_PARALLELISM = "couchbase.maxParallelism";
  public static final String COUCHBASE_BUCKET_TYPE = "couchbase.bucketType";
  public static final String COUCHBASE_STORAGE_TYPE = "couchbase.storageBackend";
  public static final String COUCHBASE_DEBUG_MODE = "couchbase.debug";
  public static final String CAPELLA_PROJECT_NAME = "capella.project.name";
  public static final String CAPELLA_DATABASE_NAME = "capella.database.name";
  public static final String CAPELLA_TOKEN = "capella.token";
  public static final String CAPELLA_USER_EMAIL = "capella.user.email";
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final Boolean DEFAULT_SSL_MODE = true;
  public static final String DEFAULT_SSL_SETTING = "true";
  private String hostname = DEFAULT_HOSTNAME;
  private String username = DEFAULT_USER;
  private String password = DEFAULT_PASSWORD;
  private String rootCert;
  private String clientCert;
  private KeyStoreType keyStoreType = KeyStoreType.PKCS12;
  private Boolean sslMode = DEFAULT_SSL_MODE;
  private Boolean enableDebug = false;
  private String bucketName;
  private String scopeName;
  private String collectionName;
  private int bucketReplicas = 1;
  private int maxParallelism = 0;
  private BucketType bucketType = BucketType.COUCHBASE;
  private StorageBackend bucketStorage = StorageBackend.COUCHSTORE;
  private int ttlSeconds = 0;
  private Boolean basic = false;
  private final Properties properties = new Properties();

  public CouchbaseConfig ttl(int value) {
    this.ttlSeconds = value;
    return this;
  }

  public CouchbaseConfig host(final String name) {
    this.hostname = name;
    return this;
  }

  public CouchbaseConfig username(final String name) {
    this.username = name;
    return this;
  }

  public CouchbaseConfig password(final String password) {
    this.password = password;
    return this;
  }

  public CouchbaseConfig bucketReplicas(final int count) {
    this.bucketReplicas = count;
    return this;
  }

  public CouchbaseConfig maxParallelism(final int count) {
    this.maxParallelism = count;
    return this;
  }

  public CouchbaseConfig bucketType(final String bucketType) {
    this.bucketType = convertBucketType(bucketType);
    return this;
  }

  public CouchbaseConfig bucketStorage(final String storageBackend) {
    this.bucketStorage = convertStorageBackend(storageBackend);
    return this;
  }

  public CouchbaseConfig rootCert(final String name) {
    this.rootCert = name;
    return this;
  }

  public CouchbaseConfig clientKeyStore(final String name) {
    this.clientCert = name;
    return this;
  }

  public CouchbaseConfig keyStoreType(final KeyStoreType type) {
    this.keyStoreType = type;
    return this;
  }

  public CouchbaseConfig connect(final String host, final String user, final String password) {
    this.hostname = host;
    this.username = user;
    this.password = password;
    return this;
  }

  public CouchbaseConfig ssl(final Boolean mode) {
    this.sslMode = mode;
    return this;
  }

  public CouchbaseConfig bucket(final String name) {
    this.bucketName = name;
    return this;
  }

  public CouchbaseConfig scope(final String scope) {
    this.scopeName = scope;
    return this;
  }

  public CouchbaseConfig collection(final String name) {
    this.collectionName = name;
    return this;
  }

  public CouchbaseConfig enableDebug(final Boolean mode) {
    this.enableDebug = mode;
    return this;
  }

  public CouchbaseConfig basic() {
    this.basic = true;
    return this;
  }

  public CouchbaseConfig capella(final String project, final String database, final String email, final String token) {
    properties.setProperty(CAPELLA_PROJECT_NAME, project);
    properties.setProperty(CAPELLA_DATABASE_NAME, database);
    properties.setProperty(CAPELLA_USER_EMAIL, email);
    properties.setProperty(CAPELLA_TOKEN, token);
    return this;
  }

  public CouchbaseConfig fromProperties(Properties properties) {
    this.hostname = properties.getProperty(COUCHBASE_HOST, DEFAULT_HOSTNAME);
    this.username = properties.getProperty(COUCHBASE_USER, DEFAULT_USER);
    this.password = properties.getProperty(COUCHBASE_PASSWORD, DEFAULT_PASSWORD);
    this.clientCert = properties.getProperty(COUCHBASE_CLIENT_CERTIFICATE);
    this.rootCert = properties.getProperty(COUCHBASE_ROOT_CERTIFICATE);
    this.keyStoreType = KeyStoreType.valueOf(properties.getProperty(COUCHBASE_KEYSTORE_TYPE, "PKCS12").toUpperCase());
    this.bucketName = properties.getProperty(COUCHBASE_BUCKET, "default");
    this.scopeName = properties.getProperty(COUCHBASE_SCOPE, "_default");
    this.collectionName = properties.getProperty(COUCHBASE_COLLECTION, "_default");
    this.sslMode = properties.getProperty(COUCHBASE_SSL_MODE, DEFAULT_SSL_SETTING).equals("true");
    this.bucketReplicas = Integer.parseInt(properties.getProperty(COUCHBASE_REPLICA_NUM, "1"));
    this.maxParallelism = Integer.parseInt(properties.getProperty(COUCHBASE_MAX_PARALLELISM, "0"));
    this.ttlSeconds = Integer.parseInt(properties.getProperty(COUCHBASE_TTL, "0"));
    this.bucketType = convertBucketType(properties.getProperty(COUCHBASE_BUCKET_TYPE, "couchbase"));
    this.bucketStorage = convertStorageBackend(properties.getProperty(COUCHBASE_STORAGE_TYPE, "couchstore"));
    this.enableDebug = properties.getProperty(COUCHBASE_DEBUG_MODE, "false").equals("true");
    this.properties.putAll(properties);
    return this;
  }

  public String getHostname() {
    return hostname;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public String getRootCert() {
    return rootCert;
  }

  public String getClientCert() {
    return clientCert;
  }

  public KeyStoreType getKeyStoreType() {
    return keyStoreType;
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

  public Boolean getSslMode() {
    return sslMode;
  }

  public Boolean getEnableDebug() {
    return enableDebug;
  }

  public int getBucketReplicas() {
    return bucketReplicas;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public BucketType getBucketType() {
    return bucketType;
  }

  public StorageBackend getBucketStorage() {
    return bucketStorage;
  }

  public int getTtlSeconds() {
    return ttlSeconds;
  }

  public Boolean getBasic() {
    return basic;
  }

  public Properties getProperties() {
    return properties;
  }
}
