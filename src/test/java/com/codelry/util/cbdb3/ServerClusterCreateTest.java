package com.codelry.util.cbdb3;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class ServerClusterCreateTest extends AbstractServerSharedTestcontainerTest {
  private static final String HOST = "127.0.0.1";
  private static final String ADMIN = CouchbaseConfig.DEFAULT_USER;
  private static final String PASSWORD = CouchbaseConfig.DEFAULT_PASSWORD;
  private static final String BUCKET = "cluster-test";

  @Test
  public void createClusterConnectAndCreateBucket() {
    CouchbaseConnect db = Server.getInstance();
    CouchbaseConfig config = new CouchbaseConfig()
        .host(HOST)
        .username(ADMIN)
        .password(PASSWORD)
        .bucket(BUCKET)
        .ssl(false);

    Map<String, String> options = Map.of(
        String.format(CouchbaseConfig.COUCHBASE_SERVER_IP, 0), HOST,
        String.format(CouchbaseConfig.COUCHBASE_SERVER_RAM, 0), "4",
        String.format(CouchbaseConfig.COUCHBASE_SERVER_SERVICES, 0), "data,index,query,fts"
    );

    db.createCluster(config, options);
    db.connect(config);

    db.createBucket(BUCKET, 256, 0);
    Assertions.assertTrue(db.isBucket(BUCKET));
    db.dropBucket(BUCKET);
    db.disconnect();
  }
}
