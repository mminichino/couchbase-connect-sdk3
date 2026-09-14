package com.codelry.util.cbdb3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.List;
import java.util.Map;

/**
 * Protostellar / Cloud Native Gateway integration test using a dedicated Couchbase
 * container plus a CNG container on a shared Docker network.
 */
public class ServerProtostellarTest extends AbstractServerTestcontainerTest {
  private static final String HOST = CouchbaseConfig.DEFAULT_HOSTNAME;
  private static final String ADMIN = CouchbaseConfig.DEFAULT_USER;
  private static final String PASSWORD = CouchbaseConfig.DEFAULT_PASSWORD;
  private static final String BUCKET = "cng-test";
  private static final String DOC_ID = "protostellar::1";

  private static Network network;
  private static GenericContainer<?> couchbase;
  private static GenericContainer<?> cng;
  private static String couchbaseNetworkIp;

  @BeforeAll
  static void startCouchbaseAndCng() throws Exception {
    loadProperties();
    network = Network.newNetwork();
    couchbase = CouchbaseServerContainer.startDedicatedContainer(
        network,
        CouchbaseServerContainer.COUCHBASE_NETWORK_ALIAS);
    couchbaseNetworkIp = CouchbaseServerContainer.containerNetworkIp(couchbase);
    LOGGER.info("Couchbase Docker network IP: {}", couchbaseNetworkIp);
    initializeCluster();
    cng = CouchbaseServerContainer.startCloudNativeGateway(
        network,
        couchbaseNetworkIp,
        ADMIN,
        PASSWORD);
  }

  @AfterAll
  static void stopContainers() {
    Server.getInstance().disconnect();
    CouchbaseServerContainer.stopContainer(cng);
    CouchbaseServerContainer.stopContainer(couchbase);
    if (network != null) {
      network.close();
    }
    cng = null;
    couchbase = null;
    network = null;
  }

  @Test
  public void upsertAndGetViaProtostellar() {
    CouchbaseConnect db = Server.getInstance();
    CouchbaseConfig config = new CouchbaseConfig()
        .host(HOST + ":" + CouchbaseServerContainer.CNG_GRPC_PORT)
        .username(ADMIN)
        .password(PASSWORD)
        .bucket(BUCKET)
        .scope("_default")
        .collection("_default")
        .protostellar(true)
        .sslVerify(false);

    db.connect(config);
    db.createBucket(BUCKET, 128, 0);
    Assertions.assertTrue(db.isBucket(BUCKET));

    db.connectKeyspace(BUCKET, "_default", "_default");

    ObjectNode doc = new ObjectMapper().createObjectNode();
    doc.put("protocol", "protostellar");
    doc.put("value", 42);
    db.upsert(DOC_ID, doc);

    JsonNode fetched = db.get(DOC_ID);
    Assertions.assertNotNull(fetched);
    Assertions.assertEquals("protostellar", fetched.path("protocol").asText());
    Assertions.assertEquals(42, fetched.path("value").asInt());

    db.dropBucket(BUCKET);
    db.disconnect();
  }

  private static void initializeCluster() {
    // Admin REST from the host uses published localhost ports; the cluster advertises
    // its Docker network IP so CNG on the same network can reach KV/Query.
    ClusterCreateSupport.ClusterRestEndpoint endpoint =
        ClusterCreateSupport.ClusterRestEndpoint.forServer(HOST, false);
    ClusterNodeConfig node = new ClusterNodeConfig()
        .setIp(couchbaseNetworkIp)
        .setRamGiB(4)
        .setServices(List.of("data", "index", "query", "fts"));
    Map<String, String> options = Map.of(
        String.format(CouchbaseConfig.COUCHBASE_SERVER_IP, 0), couchbaseNetworkIp,
        String.format(CouchbaseConfig.COUCHBASE_SERVER_RAM, 0), "4",
        String.format(CouchbaseConfig.COUCHBASE_SERVER_SERVICES, 0), "data,index,query,fts"
    );
    Map<String, Integer> quotas = ClusterCreateSupport.calculateServerQuotas(node, options);

    if (ClusterCreateSupport.isClusterInitialized(endpoint, ADMIN, PASSWORD)) {
      LOGGER.info("Cluster already initialized on {}", HOST);
    } else {
      LOGGER.info("Initializing cluster advertising hostname {}", couchbaseNetworkIp);
      ClusterCreateSupport.initializeSingleNodeCluster(
          endpoint,
          ADMIN,
          PASSWORD,
          node.getServices(),
          quotas,
          couchbaseNetworkIp);
    }
    ClusterCreateSupport.waitForCluster(endpoint, ADMIN, PASSWORD, 60);
    ClusterCreateSupport.waitForClusterServices(endpoint, ADMIN, PASSWORD);
    ClusterCreateSupport.waitForQueryReady(endpoint, ADMIN, PASSWORD);
    ClusterCreateSupport.waitForRebalanceComplete(endpoint, ADMIN, PASSWORD);
  }
}
