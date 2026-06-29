package com.codelry.util.cbdb3;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ServerDriver2Test extends AbstractServerPerClassTestcontainerTest {

  @Test
  public void runTest() {
    CouchbaseConnect db = Server.getInstance();
    CouchbaseConfig config = serverConfig();
    db.connect(config);

    boolean result = db.isBucket();
    LOGGER.debug("isBucket: {}", result);
    db.createBucket();
    result = db.isBucket();
    Assertions.assertTrue(result);
    db.createScope();
    db.createCollection();
    db.clusterWait();
    db.createPrimaryIndex();
    db.createSecondaryIndex("idx_test", List.of("data"));
    ObjectNode doc = new ObjectMapper().createObjectNode();
    doc.put("data", 1);
    db.connectKeyspace();
    db.upsert("doc::1", doc);
    db.dropBucket();
    db.disconnect();
  }
}
