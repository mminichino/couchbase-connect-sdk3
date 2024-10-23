package com.codelry.util.cbdb3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

public class CapellaDriver1Test {
  private static final Logger LOGGER = LogManager.getLogger(CapellaDriver1Test.class);
  private static final String propertyFile = "test.capella.properties";
  public static Properties properties;
  public static final String CLUSTER_HOST = "couchbase.hostname";
  public static final String CLUSTER_USER = "couchbase.username";
  public static final String CLUSTER_PASSWORD = "couchbase.password";
  public static final String CLUSTER_BUCKET = "couchbase.bucket";
  public static final String COUCHBASE_SCOPE = "couchbase.scope";
  public static final String COUCHBASE_COLLECTION = "couchbase.collection";
  public static final String CAPELLA_PROJECT_NAME = "capella.project.name";
  public static final String CAPELLA_DATABASE_NAME = "capella.database.name";
  public static final String CAPELLA_USER_EMAIL = "capella.user.email";
  public static final String CAPELLA_TOKEN = "capella.token";
  public static final String DEFAULT_USER = "Administrator";
  public static final String DEFAULT_PASSWORD = "password";
  public static final String DEFAULT_HOSTNAME = "127.0.0.1";
  public static final String DEFAULT_BUCKET = "test";
  public static final String DEFAULT_SCOPE = "test";
  public static final String DEFAULT_COLLECTION = "userdata";
  public static final String DEFAULT_PROJECT_NAME = "junit";
  public static final String DEFAULT_DATABASE_NAME = "testdb";

  @BeforeAll
  public static void setUpBeforeClass() {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    properties = new Properties();

    LOGGER.info("Testing with properties file: {}", propertyFile);
    try {
      properties.load(loader.getResourceAsStream(propertyFile));
    } catch (IOException e) {
      LOGGER.debug("can not open properties file: {}", e.getMessage(), e);
    }
  }

  @Test
  public void testBasic1() {
    String hostname = properties.getProperty(CLUSTER_HOST, DEFAULT_HOSTNAME);
    String username = properties.getProperty(CLUSTER_USER, DEFAULT_USER);
    String password = properties.getProperty(CLUSTER_PASSWORD, DEFAULT_PASSWORD);
    String bucket = properties.getProperty(CLUSTER_BUCKET, DEFAULT_BUCKET);
    String scope = properties.getProperty(COUCHBASE_SCOPE, DEFAULT_SCOPE);
    String collection = properties.getProperty(COUCHBASE_COLLECTION, DEFAULT_COLLECTION);
    String project = properties.getProperty(CAPELLA_PROJECT_NAME, DEFAULT_PROJECT_NAME);
    String database = properties.getProperty(CAPELLA_DATABASE_NAME, DEFAULT_DATABASE_NAME);
    String email = properties.getProperty(CAPELLA_USER_EMAIL);
    String token = properties.getProperty(CAPELLA_TOKEN);

    LOGGER.info("hostname: {}", hostname);
    LOGGER.info("username: {}", username);

    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db = dbBuilder
        .host(hostname)
        .username(username)
        .password(password)
        .capella(project, database, email, token)
        .build();
    System.out.println(db.clusterVersion);
    boolean result = db.isBucket(bucket);
    Assertions.assertFalse(result);
    db.createBucket(bucket);
    result = db.isBucket(bucket);
    Assertions.assertTrue(result);
    db.createScope(bucket, scope);
    db.createCollection(bucket, scope, collection);
  }
}
