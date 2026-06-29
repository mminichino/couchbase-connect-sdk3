package com.codelry.util.cbdb3;

import com.codelry.util.capella.*;
import com.codelry.util.capella.exceptions.CapellaAPIError;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

@TestMethodOrder(OrderAnnotation.class)
public class CapellaDriver2Test {
  private static final Logger LOGGER = LogManager.getLogger(CapellaDriver2Test.class);
  private static final String propertyFile = "test.capella.2.properties";
  public static Properties properties;
  public String database = "testdb";
  public String username = "developer";
  public String password = "#C0uchBas3";

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
  @Order(1)
  public void createCluster() throws CapellaAPIError {
    CouchbaseCapella capella = CouchbaseCapella.getInstance(properties);
    CapellaOrganization organization = CapellaOrganization.getInstance(capella);
    CapellaProject project = organization.getDefaultProject();
    CapellaCluster cluster = project.createCluster(new CapellaCluster.ClusterConfig());
    Assertions.assertNotNull(cluster);
    CapellaAllowedCIDR cidr = cluster.getAllowedCIDR();
    cidr.createAllowedCIDR("0.0.0.0/0");
    CapellaCredentials user = cluster.getCredentials();
    user.createCredential(username, password, null);
    Assertions.assertTrue(new CapellaConnectivity().checkConnectivity(cluster.getConnectString(), Duration.ofMinutes(2)));
  }

  @Test
  @Order(2)
  public void testBasic1() {
    CouchbaseConnect db = Capella.getInstance();
    CouchbaseConfig config = new CouchbaseConfig().fromProperties(properties);
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

  @Test
  @Order(3)
  public void dropCluster() throws CapellaAPIError {
    CouchbaseCapella capella = CouchbaseCapella.getInstance(properties);
    CapellaOrganization organization = CapellaOrganization.getInstance(capella);
    CapellaProject project = organization.getDefaultProject();
    CapellaCluster cluster = project.createCluster(new CapellaCluster.ClusterConfig());
    cluster.delete();
  }
}
