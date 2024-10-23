package com.codelry.util.cbdb3;

import com.codelry.util.capella.*;
import com.codelry.util.capella.exceptions.CapellaAPIError;
import com.codelry.util.capella.logic.AvailabilityType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

public class CreateClusterTest {
  private static final Logger LOGGER = LogManager.getLogger(CreateClusterTest.class);
  private static final String propertyFile = "test.cluster.properties";
  public static Properties properties;
  public String allowedCIDR = "0.0.0.0/0";
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
  public void testDatabaseCreate() throws CapellaAPIError {
    CouchbaseCapella capella = CouchbaseCapella.getInstance(properties);
    CapellaOrganization organization = CapellaOrganization.getInstance(capella);
    CapellaProject project = CapellaProject.getInstance(organization);
    Assertions.assertNotNull(project.getId());
    CapellaCluster cluster = CapellaCluster.getInstance(project, new CapellaCluster.ClusterConfig().singleNode());
    CapellaAllowedCIDR cidr = CapellaAllowedCIDR.getInstance(cluster);
    cidr.createAllowedCIDR(allowedCIDR);
    CapellaCredentials user = CapellaCredentials.getInstance(cluster);
    user.createCredential(username, password, new ObjectMapper().createArrayNode());
    System.out.println(cluster.getConnectString());
  }
}
