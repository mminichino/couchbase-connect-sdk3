package com.codelry.util.cbdb3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ServerDriver1Test extends AbstractServerPerClassTestcontainerTest {

  @Test
  public void runTest() {
    String username = properties.getProperty(CouchbaseConfig.COUCHBASE_USER, CouchbaseConfig.DEFAULT_USER);
    String password = properties.getProperty(CouchbaseConfig.COUCHBASE_PASSWORD, CouchbaseConfig.DEFAULT_PASSWORD);
    String bucket = properties.getProperty(CouchbaseConfig.COUCHBASE_BUCKET, "default");
    String scope = properties.getProperty(CouchbaseConfig.COUCHBASE_SCOPE, "_default");
    String collection = properties.getProperty(CouchbaseConfig.COUCHBASE_COLLECTION, "_default");
    String hostname = properties.getProperty(CouchbaseConfig.COUCHBASE_HOST, CouchbaseConfig.DEFAULT_HOSTNAME);

    LOGGER.info("hostname: {}", hostname);
    LOGGER.info("username: {}", username);

    CouchbaseConnect db = Server.getInstance();
    CouchbaseConfig config = serverConfig()
        .username(username)
        .password(password);
    db.connect(config);

    boolean result = db.isBucket(bucket);
    LOGGER.debug("isBucket: {}", result);
    db.createBucket(bucket);
    result = db.isBucket(bucket);
    Assertions.assertTrue(result);
    db.createScope(bucket, scope);
    db.createCollection(bucket, scope, collection);
    db.clusterWait();
    db.createPrimaryIndex(bucket, scope, collection);
    db.createSecondaryIndex(bucket, scope, collection, "idx_test", List.of("data"));
    ObjectNode doc = new ObjectMapper().createObjectNode();
    doc.put("data", 1);
    db.connectBucket(bucket);
    db.connectCollection(scope, collection);
    db.upsert("doc::1", doc);
    db.dropBucket(bucket);
    db.disconnect();
  }
}
