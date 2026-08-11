package com.codelry.util.cbdb3;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

class CouchbaseConnectTest {

  @Test
  void resolveCapellaFromPropertiesFile() throws IOException {
    Properties properties = loadProperties("test.capella.2.properties");
    CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);

    Assertions.assertTrue(config.isCapella());
    Assertions.assertSame(Capella.getInstance(), CouchbaseConnect.resolve(config));
  }

  @Test
  void resolveServerFromPropertiesFile() throws IOException {
    Properties properties = loadProperties("test.server.properties");
    CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);

    Assertions.assertFalse(config.isCapella());
    Assertions.assertSame(Server.getInstance(), CouchbaseConnect.resolve(config));
  }

  @Test
  void getInstanceReturnsAutoRouter() {
    Assertions.assertSame(AutoCouchbaseConnect.getInstance(), CouchbaseConnect.getInstance());
  }

  @Test
  void getClusterRequiresConnectFirst() {
    CouchbaseConnect db = CouchbaseConnect.getInstance();
    Assertions.assertThrows(IllegalStateException.class, db::getCluster);
  }

  @Test
  void maxHttpConnectionsDefaultsTo64() {
    Assertions.assertEquals(64, new CouchbaseConfig().getMaxHttpConnections());
  }

  @Test
  void maxHttpConnectionsFromProperties() {
    Properties properties = new Properties();
    properties.setProperty(CouchbaseConfig.COUCHBASE_MAX_HTTP_CONNECTIONS, "32");
    CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);
    Assertions.assertEquals(32, config.getMaxHttpConnections());
  }

  @Test
  void maxHttpConnectionsFluentSetter() {
    CouchbaseConfig config = new CouchbaseConfig().maxHttpConnections(128);
    Assertions.assertEquals(128, config.getMaxHttpConnections());
  }

  private static Properties loadProperties(String resourceName) throws IOException {
    Properties properties = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    properties.load(loader.getResourceAsStream(resourceName));
    return properties;
  }
}
