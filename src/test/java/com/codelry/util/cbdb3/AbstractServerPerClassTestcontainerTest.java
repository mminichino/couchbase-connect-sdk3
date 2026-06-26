package com.codelry.util.cbdb3;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;

import java.util.Map;

/**
 * Base for test classes that start a dedicated Couchbase container for the class
 * and stop it when the class finishes.
 */
abstract class AbstractServerPerClassTestcontainerTest extends AbstractServerTestcontainerTest {
  protected static GenericContainer<?> couchbase;

  @BeforeAll
  static void startContainerAndCluster() throws Exception {
    loadProperties();
    couchbase = CouchbaseServerContainer.startDedicatedContainer();
    initializeCluster();
  }

  @AfterAll
  static void stopContainer() {
    Server.getInstance().disconnect();
    CouchbaseServerContainer.stopContainer(couchbase);
    couchbase = null;
  }

  private static void initializeCluster() {
    String hostname = properties.getProperty(CouchbaseConfig.COUCHBASE_HOST, CouchbaseConfig.DEFAULT_HOSTNAME);
    CouchbaseConnect db = Server.getInstance();
    CouchbaseConfig config = serverConfig();
    Map<String, String> options = Map.of(
        String.format(CouchbaseConfig.COUCHBASE_SERVER_IP, 0), hostname,
        String.format(CouchbaseConfig.COUCHBASE_SERVER_RAM, 0), "4",
        String.format(CouchbaseConfig.COUCHBASE_SERVER_SERVICES, 0), "data,index,query,fts"
    );
    db.createCluster(config, options);
    ClusterCreateSupport.waitForClusterServices(
        hostname,
        8091,
        config.getUsername(),
        config.getPassword());
    ClusterCreateSupport.waitForQueryReady(
        hostname,
        config.getUsername(),
        config.getPassword());
    ClusterCreateSupport.waitForRebalanceComplete(
        hostname,
        8091,
        config.getUsername(),
        config.getPassword());
    Server.getInstance().disconnect();
  }
}
