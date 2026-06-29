package com.codelry.util.cbdb3;

import org.junit.jupiter.api.BeforeAll;

import java.util.Map;

abstract class AbstractServerInitializedTest extends AbstractServerSharedTestcontainerTest {
  private static volatile boolean clusterReady;

  @BeforeAll
  static void setUpCluster() throws Exception {
    loadProperties();
    if (clusterReady) {
      return;
    }
    synchronized (AbstractServerInitializedTest.class) {
      if (clusterReady) {
        return;
      }
      String hostname = properties.getProperty(CouchbaseConfig.COUCHBASE_HOST, CouchbaseConfig.DEFAULT_HOSTNAME);
      ClusterCreateSupport.ClusterRestEndpoint endpoint =
          ClusterCreateSupport.ClusterRestEndpoint.forServer(hostname, false);
      String username = properties.getProperty(CouchbaseConfig.COUCHBASE_USER, CouchbaseConfig.DEFAULT_USER);
      String password = properties.getProperty(CouchbaseConfig.COUCHBASE_PASSWORD, CouchbaseConfig.DEFAULT_PASSWORD);
      if (ClusterCreateSupport.isClusterInitialized(endpoint, username, password)) {
        ClusterCreateSupport.waitForClusterServices(endpoint, username, password);
        ClusterCreateSupport.waitForQueryReady(endpoint, username, password);
        ClusterCreateSupport.waitForRebalanceComplete(endpoint, username, password);
        clusterReady = true;
        return;
      }
      CouchbaseConnect db = Server.getInstance();
      CouchbaseConfig config = serverConfig();
      Map<String, String> options = Map.of(
          String.format(CouchbaseConfig.COUCHBASE_SERVER_IP, 0), hostname,
          String.format(CouchbaseConfig.COUCHBASE_SERVER_RAM, 0), "4",
          String.format(CouchbaseConfig.COUCHBASE_SERVER_SERVICES, 0), "data,index,query,fts"
      );
      db.createCluster(config, options);
      ClusterCreateSupport.waitForClusterServices(endpoint, config.getUsername(), config.getPassword());
      ClusterCreateSupport.waitForQueryReady(endpoint, config.getUsername(), config.getPassword());
      ClusterCreateSupport.waitForRebalanceComplete(endpoint, config.getUsername(), config.getPassword());
      Server.getInstance().disconnect();
      clusterReady = true;
    }
  }
}
