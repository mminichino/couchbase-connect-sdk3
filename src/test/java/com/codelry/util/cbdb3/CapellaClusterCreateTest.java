package com.codelry.util.cbdb3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

public class CapellaClusterCreateTest {
  private static final Logger LOGGER = LogManager.getLogger(CapellaClusterCreateTest.class);
  private static final String propertyFile = "test.capella.2.properties";
  private static Properties properties;
  private CouchbaseConnect db;

  @BeforeAll
  public static void setUpBeforeClass() throws IOException {
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    properties = new Properties();
    properties.load(loader.getResourceAsStream(propertyFile));
    LOGGER.info("Testing with properties file: {}", propertyFile);
  }

  @AfterEach
  public void tearDown() {
    if (db != null) {
      try {
        db.destroyCluster();
      } catch (Exception e) {
        LOGGER.warn("Failed to destroy Capella cluster during cleanup", e);
      }
      db.disconnect();
      db = null;
    }
  }

  @Test
  public void createClusterConnectCreateBucketAndDestroy() {
    CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);
    db = Capella.getInstance();

    db.createCluster(config);
    db.connect(config);

    String bucket = config.getBucketName();
    db.createBucket(bucket, 128, 0);
    Assertions.assertTrue(db.isBucket(bucket));
    db.clusterWait();

    db.dropBucket(bucket);
    db.destroyCluster();
    db.disconnect();
    db = null;
  }
}
