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
            .numKvConnections(4)
            .networkResolution(NetworkResolution.AUTO)
            .enableMutationTokens(false);

        Consumer<TimeoutConfig.Builder> timeOutConfiguration = timeoutConfig -> timeoutConfig
            .kvTimeout(Duration.ofSeconds(5))
            .connectTimeout(Duration.ofSeconds(15))
            .queryTimeout(Duration.ofSeconds(120));

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
  protected String streamHostname() {
    return connectTarget;
  }

  @Override
  protected boolean supportsRbacRest() {
    return true;
  }
}
