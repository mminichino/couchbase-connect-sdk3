package com.codelry.util.cbdb3;

import com.codelry.util.capella.CapellaBucket;
import com.codelry.util.capella.CapellaCluster;
import com.codelry.util.capella.CapellaConnect;
import com.codelry.util.capella.CapellaCredentials;
import com.codelry.util.capella.CapellaOrganization;
import com.codelry.util.capella.CapellaProject;
import com.codelry.util.capella.CouchbaseCapella;
import com.codelry.util.capella.exceptions.CapellaAPIError;
import com.codelry.util.capella.exceptions.NotFoundException;
import com.couchbase.client.java.manager.bucket.BucketSettings;

/**
 * Couchbase Capella connection using the Capella API to resolve the connect string.
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
      if (cluster == null) {
        logger.info("Connecting to Couchbase Capella database {}", databaseName);

        CouchbaseCapella capellaApi = CouchbaseCapella.getInstance(properties);
        CapellaOrganization organization = CapellaOrganization.getInstance(capellaApi);
        CapellaProject project = CapellaProject.getInstance(organization);
        capellaCluster = CapellaCluster.getInstance(project);

        CapellaCredentials credentials = capellaCluster.getCredentials();
        credentials.createCredential(username, password, null);
        cluster = CapellaConnect.connect(capellaCluster);
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
  protected String streamHostname() {
    return streamHost;
  }

  @Override
  protected boolean supportsRbacRest() {
    return false;
  }
}
