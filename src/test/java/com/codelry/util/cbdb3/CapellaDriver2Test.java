package com.codelry.util.cbdb3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

public class CapellaDriver2Test {
  private static final Logger LOGGER = LogManager.getLogger(CapellaDriver2Test.class);
  private static final String propertyFile = "test.capella.properties";
  public static Properties properties;
  public static final String DEFAULT_BUCKET = "test";
  public static final String DEFAULT_SCOPE = "test";
  public static final String DEFAULT_COLLECTION = "userdata";

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
    CouchbaseConnect.CouchbaseBuilder dbBuilder = new CouchbaseConnect.CouchbaseBuilder();
    CouchbaseConnect db = dbBuilder
        .fromProperties(properties)
        .bucketReplicas(0)
        .build();
    System.out.println(db.clusterVersion);
    boolean result = db.isBucket();
    Assertions.assertNotNull(result);
    db.createBucket();
    result = db.isBucket();
    Assertions.assertTrue(result);
    db.createScope();
    db.createCollection();
    ObjectNode doc = new ObjectMapper().createObjectNode();
    doc.put("data", 1);
    db.connectKeyspace();
    db.upsert("doc::1", doc);
    db.dropBucket();
  }
}
