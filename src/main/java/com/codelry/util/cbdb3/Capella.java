package com.codelry.util.cbdb3;

import com.codelry.util.capella.*;
import com.codelry.util.capella.exceptions.CapellaAPIError;
import com.codelry.util.capella.exceptions.NotFoundException;
import com.codelry.util.capella.logic.BucketData;
import com.couchbase.client.java.manager.bucket.BucketSettings;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Couchbase Capella connection using the Capella API to resolve the connection string.
 */
public final class Capella extends AbstractCouchbaseConnect {
  private static Capella instance;
  private CapellaCluster capellaCluster;
  private String databaseName;
  private String streamHost;

  private Capella() {}

  public static Capella getInstance() {
    if (instance == null) {
      instance = new Capella();
    }
    return instance;
  }

  @Override
  public void connect(CouchbaseConfig config) {
    applyConfig(config);
    enableDebugLogging();
    useSsl = true;
    adminPort = 18091;
    boolean softFailure = config.getSoftFailure();

    resolveDatabaseName(config);
    validateCapellaConfig();

    try {
      if (cluster != null && capellaCluster == null) {
        CapellaConnect.disconnect(cluster);
        cluster = null;
      }
      if (cluster == null) {
        logger.info("Connecting to Couchbase Capella database {}", databaseName);

        CouchbaseCapella capellaApi = CouchbaseCapella.getInstance(properties);
        CapellaOrganization organization = CapellaOrganization.getInstance(capellaApi);
        CapellaProject project = CapellaProject.getInstance(organization);
        capellaCluster = CapellaCluster.getInstance(project, databaseName);
        capellaCluster.getCredentials().addCredentials(username, password);
        CapellaClusterConfig capellaConfig = new CapellaClusterConfig()
            .kvEndpoints(kvEndpoints)
            .kvTimeout(kvTimeout)
            .connectTimeout(connectTimeout)
            .queryTimeout(queryTimeout)
            .build();
        cluster = CapellaConnect.connect(capellaCluster, capellaConfig);
        connectTarget = databaseName;
        streamHost = extractHost(capellaCluster.getConnectString());

        logger.debug("Capella cluster connected for database {}", databaseName);
        finishConnect(config);
      }
    } catch (Exception e) {
      logError(e, capellaCluster != null ? capellaCluster.getConnectString() : databaseName);
      if (!softFailure) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  private void resolveDatabaseName(CouchbaseConfig config) {
    databaseName = properties.getProperty(CouchbaseConfig.CAPELLA_DATABASE_NAME);
    if (databaseName == null || databaseName.isBlank()) {
      databaseName = properties.getProperty(CouchbaseConfig.CAPELLA_DATABASE_ID);
    }
    if (databaseName == null || databaseName.isBlank()) {
      databaseName = connectTarget;
    }
    properties.setProperty(CouchbaseConfig.CAPELLA_DATABASE_NAME, databaseName);
    connectTarget = databaseName;
  }

  private void validateCapellaConfig() {
    if (!capellaTokenSet()) {
      throw new IllegalArgumentException("Capella connection requires " + CouchbaseConfig.CAPELLA_TOKEN);
    }
    if (!capellaProjectSet()) {
      throw new IllegalArgumentException("Capella connection requires "
          + CouchbaseConfig.CAPELLA_PROJECT_NAME + " or " + CouchbaseConfig.CAPELLA_PROJECT_ID);
    }
    if (!capellaDatabaseSet()) {
      throw new IllegalArgumentException("Capella connection requires "
          + CouchbaseConfig.CAPELLA_DATABASE_NAME + " or " + CouchbaseConfig.CAPELLA_DATABASE_ID);
    }
    if (!capellaUserSet()) {
      throw new IllegalArgumentException("Capella connection requires "
          + CouchbaseConfig.CAPELLA_USER_EMAIL + " or " + CouchbaseConfig.CAPELLA_USER_ID);
    }
  }

  private boolean capellaTokenSet() {
    return properties.getProperty(CouchbaseConfig.CAPELLA_TOKEN) != null;
  }

  private boolean capellaProjectSet() {
    return properties.getProperty(CouchbaseConfig.CAPELLA_PROJECT_ID) != null
        || properties.getProperty(CouchbaseConfig.CAPELLA_PROJECT_NAME) != null;
  }

  private boolean capellaDatabaseSet() {
    return properties.getProperty(CouchbaseConfig.CAPELLA_DATABASE_ID) != null
        || properties.getProperty(CouchbaseConfig.CAPELLA_DATABASE_NAME) != null;
  }

  private boolean capellaUserSet() {
    return properties.getProperty(CouchbaseConfig.CAPELLA_USER_EMAIL) != null
        || properties.getProperty(CouchbaseConfig.CAPELLA_USER_ID) != null;
  }

  private static String extractHost(String connectString) {
    if (connectString == null || connectString.isBlank()) {
      return "";
    }
    String stripped = connectString;
    if (stripped.startsWith("couchbases://")) {
      stripped = stripped.substring("couchbases://".length());
    } else if (stripped.startsWith("couchbase://")) {
      stripped = stripped.substring("couchbase://".length());
    }
    int comma = stripped.indexOf(',');
    if (comma >= 0) {
      stripped = stripped.substring(0, comma);
    }
    int query = stripped.indexOf('?');
    if (query >= 0) {
      stripped = stripped.substring(0, query);
    }
    return stripped;
  }

  @Override
  public void disconnect() {
    bucket = null;
    if (cluster != null) {
      CapellaConnect.disconnect(cluster);
    }
    cluster = null;
    capellaCluster = null;
    clusterInfo = mapper.createObjectNode();
  }

  @Override
  public String hostValue() {
    return databaseName;
  }

  @Override
  protected int getMemQuota() {
    return 128;
  }

  @Override
  protected void loadClusterInfo() {
    majorRevision = 7;
    minorRevision = 0;
    patchRevision = 0;
  }

  @Override
  public List<String> listBuckets() {
    if (capellaCluster == null) {
      throw new RuntimeException("Capella cluster is not connected");
    }
    try {
      List<String> names = new ArrayList<>();
      for (BucketData bucketData : CapellaBucket.getInstance(capellaCluster).list()) {
        names.add(bucketData.name());
      }
      return names;
    } catch (CapellaAPIError e) {
      throw new RuntimeException("Failed to list Capella buckets", e);
    }
  }

  @Override
  protected void createBucketImpl(BucketSettings bucketSettings) {
    try {
      CapellaBucket bucket = CapellaBucket.getInstance(capellaCluster);
      bucket.createBucket(bucketSettings);
    } catch (CapellaAPIError e) {
      logger.error("bucketCreate: Capella API error", e);
      throw new RuntimeException("bucketCreate: Capella API error", e);
    }
  }

  @Override
  protected void dropBucketImpl(String name) {
    try {
      CapellaBucket bucket = CapellaBucket.getInstance(capellaCluster, name);
      bucket.delete();
    } catch (NotFoundException e) {
      logger.debug("Drop: Bucket {} does not exist", name);
    } catch (CapellaAPIError e) {
      logger.error("dropBucket: Capella API error", e);
      throw new RuntimeException("dropBucket: Capella API error", e);
    }
  }

  @Override
  public void dropBucket(String name) {
    dropBucketImpl(name);
  }

  @Override
  protected void createClusterImpl(CouchbaseConfig config, Map<String, String> options) {
    Map<String, String> merged = ClusterCreateSupport.mergeOptions(config, options);
    properties.putAll(merged);
    resolveDatabaseName(config);
    validateCapellaConfig();

    try {
      CouchbaseCapella capellaApi = CouchbaseCapella.getInstance(properties);
      CapellaOrganization organization = CapellaOrganization.getInstance(capellaApi);
      CapellaProject project = CapellaProject.getInstance(organization);
      List<CapellaNodeConfig> nodes = ClusterCreateSupport.parseCapellaNodes(merged);
      CapellaCluster.ClusterConfig clusterConfig = ClusterCreateSupport.buildCapellaClusterConfig(merged, nodes);
      capellaCluster = project.createCluster(databaseName, clusterConfig);

      String allowedCidr = merged.getOrDefault(CouchbaseConfig.CAPELLA_CLUSTER_ALLOW, "0.0.0.0/0");
      capellaCluster.getAllowedCIDR().createAllowedCIDR(allowedCidr);
      capellaCluster.getCredentials().createCredential(username, password, null);

      if (!new CapellaConnectivity().checkConnectivity(capellaCluster.getConnectString(), Duration.ofMinutes(5))) {
        throw new RuntimeException("Capella cluster connectivity check failed");
      }
      streamHost = extractHost(capellaCluster.getConnectString());
      connectTarget = databaseName;
      ClusterCreateSupport.ClusterRestEndpoint endpoint =
          ClusterCreateSupport.ClusterRestEndpoint.forCapella(streamHost);
      ClusterCreateSupport.waitForClusterServices(endpoint, username, password);
      ClusterCreateSupport.waitForQueryReady(endpoint, username, password);
      ClusterCreateSupport.waitForRebalanceComplete(endpoint, username, password);
      logger.info("Capella cluster {} created", databaseName);
    } catch (CapellaAPIError e) {
      throw new RuntimeException("Failed to create Capella cluster", e);
    }
  }

  @Override
  protected void destroyClusterImpl() {
    if (capellaCluster == null) {
      return;
    }
    try {
      capellaCluster.delete();
      logger.info("Capella cluster {} destroyed", databaseName);
    } catch (CapellaAPIError e) {
      throw new RuntimeException("Failed to destroy Capella cluster", e);
    } finally {
      capellaCluster = null;
      bucket = null;
      if (cluster != null) {
        CapellaConnect.disconnect(cluster);
        cluster = null;
      }
      streamHost = null;
    }
  }

  @Override
  protected String streamHostname() {
    return streamHost;
  }

  @Override
  protected ClusterCreateSupport.ClusterRestEndpoint clusterRestEndpoint() {
    return ClusterCreateSupport.ClusterRestEndpoint.forCapella(streamHost);
  }

  @Override
  protected boolean supportsRbacRest() {
    return false;
  }
}
