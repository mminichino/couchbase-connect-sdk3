package com.codelry.util.cbdb3;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.NetworkResolution;
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.BucketExistsException;
import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.core.deps.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.couchbase.client.java.manager.bucket.BucketSettings;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Couchbase Server connection using a hostname-based connect string.
 */
public final class Server extends AbstractCouchbaseConnect {
  private static Server instance;
  private volatile ClusterEnvironment environment;

  private Server() {}

  public static Server getInstance() {
    if (instance == null) {
      instance = new Server();
    }
    return instance;
  }

  @Override
  public void connect(CouchbaseConfig config) {
    applyConfig(config);
    enableDebugLogging();

    String couchbasePrefix = useSsl ? "couchbases://" : "couchbase://";
    adminPort = useSsl ? 18091 : 8091;
    String connectString = couchbasePrefix + connectTarget;
    boolean softFailure = config.getSoftFailure();

    try {
      if (cluster == null) {
        String clientCert = config.getClientCert();
        KeyStoreType keyStoreType = config.getKeyStoreType();
        String rootCert = config.getRootCert();

        Consumer<SecurityConfig.Builder> secConfiguration;
        if (rootCert != null) {
          secConfiguration = securityConfig -> securityConfig
              .enableTls(true)
              .trustCertificate(Paths.get(rootCert));
        } else {
          secConfiguration = securityConfig -> securityConfig
              .enableTls(useSsl)
              .enableHostnameVerification(false)
              .trustManagerFactory(InsecureTrustManagerFactory.INSTANCE);
        }

        Consumer<IoConfig.Builder> ioConfiguration = ioConfig -> ioConfig
            .numKvConnections(kvEndpoints)
            .networkResolution(NetworkResolution.AUTO)
            .enableMutationTokens(false);

        Consumer<TimeoutConfig.Builder> timeOutConfiguration = timeoutConfig -> timeoutConfig
            .kvTimeout(Duration.ofSeconds(kvTimeout))
            .connectTimeout(Duration.ofSeconds(connectTimeout))
            .queryTimeout(Duration.ofSeconds(queryTimeout));

        Authenticator authenticator;
        if (clientCert != null) {
          KeyStore keyStore = KeyStore.getInstance(keyStoreType.name());
          keyStore.load(Files.newInputStream(Paths.get(clientCert)), password.toCharArray());
          authenticator = CertificateAuthenticator.fromKeyStore(keyStore, password);
        } else {
          authenticator = PasswordAuthenticator.create(username, password);
        }

        logger.debug("connecting as user {}", username);

        environment = ClusterEnvironment.builder()
            .timeoutConfig(timeOutConfiguration)
            .ioConfig(ioConfiguration)
            .securityConfig(secConfiguration)
            .build();

        cluster = Cluster.connect(connectString,
            ClusterOptions.clusterOptions(authenticator).environment(environment));

        logger.debug("{} cluster connected", connectTarget);
        finishConnect(config);
      }
    } catch (Exception e) {
      logError(e, connectString);
      if (!softFailure) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  @Override
  public void disconnect() {
    bucket = null;
    if (cluster != null) {
      cluster.disconnect();
    }
    cluster = null;
    clusterInfo = mapper.createObjectNode();
  }

  @Override
  public String hostValue() {
    return connectTarget;
  }

  @Override
  protected void createBucketImpl(BucketSettings bucketSettings) {
    try {
      BucketManager bucketMgr = cluster.buckets();
      bucketMgr.createBucket(bucketSettings);
    } catch (BucketExistsException e) {
      logger.debug("bucketCreate: Bucket {} already exists", bucketSettings.name());
    }
  }

  @Override
  protected void dropBucketImpl(String name) {
    try {
      BucketManager bucketMgr = cluster.buckets();
      bucketMgr.dropBucket(name);
    } catch (BucketNotFoundException e) {
      logger.debug("Drop: Bucket {} does not exist", name);
    }
  }

  @Override
  public void dropBucket(String name) {
    dropBucketImpl(name);
  }

  @Override
  protected void createClusterImpl(CouchbaseConfig config, Map<String, String> options) {
    Map<String, String> merged = ClusterCreateSupport.mergeOptions(config, options);
    List<ClusterNodeConfig> nodes = ClusterCreateSupport.parseServerNodes(merged);
    if (nodes.isEmpty()) {
      throw new IllegalArgumentException("At least one Couchbase Server node must be configured");
    }

    int adminRestPort = Boolean.FALSE.equals(config.getSslMode()) ? 8091 : 18091;
    boolean useSsl = !Boolean.FALSE.equals(config.getSslMode());
    List<String> nodeHosts = new ArrayList<>();
    try {
      ClusterNodeConfig firstNode = nodes.get(0);
      ClusterCreateSupport.HostPort firstHost = ClusterCreateSupport.parseHostPort(firstNode.getIp(), adminRestPort);
      ClusterCreateSupport.ClusterRestEndpoint endpoint =
          ClusterCreateSupport.ClusterRestEndpoint.forServer(firstHost.host(), useSsl);
      if (ClusterCreateSupport.isClusterInitialized(endpoint, config.getUsername(), config.getPassword())) {
        logger.debug("Cluster already initialized on {}", firstHost.host());
        connectTarget = firstHost.host();
        ClusterCreateSupport.waitForRebalanceComplete(endpoint, config.getUsername(), config.getPassword());
        return;
      }
      Map<String, Integer> quotas = ClusterCreateSupport.calculateServerQuotas(firstNode, merged);
      logger.debug("Creating single-node cluster on {} with quotas {}", firstHost.host(), quotas);
      ClusterCreateSupport.initializeSingleNodeCluster(
          endpoint,
          config.getUsername(),
          config.getPassword(),
          firstNode.getServices(),
          quotas);
      nodeHosts.add(firstHost.host());

      for (int index = 1; index < nodes.size(); index++) {
        ClusterNodeConfig node = nodes.get(index);
        ClusterCreateSupport.HostPort nodeHost = ClusterCreateSupport.parseHostPort(node.getIp(), adminRestPort);
        ClusterCreateSupport.addNodeToCluster(
            endpoint,
            config.getUsername(),
            config.getPassword(),
            nodeHost.host(),
            node.getServices());
        nodeHosts.add(nodeHost.host());
      }

      if (nodes.size() > 1) {
        ClusterCreateSupport.rebalanceCluster(
            endpoint,
            config.getUsername(),
            config.getPassword(),
            nodeHosts);
      }

      ClusterCreateSupport.waitForCluster(endpoint, config.getUsername(), config.getPassword(), 60);
      ClusterCreateSupport.waitForRebalanceComplete(endpoint, config.getUsername(), config.getPassword());
      ClusterCreateSupport.waitForClusterServices(endpoint, config.getUsername(), config.getPassword());
      ClusterCreateSupport.waitForQueryReady(endpoint, config.getUsername(), config.getPassword());
      connectTarget = firstHost.host();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Couchbase Server cluster", e);
    }
  }

  @Override
  protected void destroyClusterImpl() {
    logger.debug("destroyCluster is not supported for Couchbase Server");
  }

  @Override
  protected String streamHostname() {
    return connectTarget;
  }

  @Override
  protected boolean supportsRbacRest() {
    return true;
  }
}
