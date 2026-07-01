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

  private static Properties loadProperties(String resourceName) throws IOException {
    Properties properties = new Properties();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    properties.load(loader.getResourceAsStream(resourceName));
    return properties;
  }
}
