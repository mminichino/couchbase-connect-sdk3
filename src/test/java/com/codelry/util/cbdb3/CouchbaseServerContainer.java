package com.codelry.util.cbdb3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

final class CouchbaseServerContainer {
  static final String IMAGE = "couchbase/server:enterprise-8.0.3";
  static final String CNG_IMAGE = "couchbase/cloud-native-gateway:1.2.1-dockerhub";
  static final String SHARED_CONTAINER_NAME = "couchbase-connect-sdk3-test";
  static final String COUCHBASE_NETWORK_ALIAS = "couchbase";
  static final int CNG_GRPC_PORT = 18098;
  static final int CNG_WEB_PORT = 9091;
  static final int CNG_DAPI_PORT = 18100;
  private static final Logger LOGGER = LoggerFactory.getLogger(CouchbaseServerContainer.class);
  private static GenericContainer<?> shared;

  private CouchbaseServerContainer() {}

  static GenericContainer<?> sharedContainer() {
    if (shared == null) {
      synchronized (CouchbaseServerContainer.class) {
        if (shared == null) {
          shared = createShared();
          if (clusterAlreadyRunning()) {
            LOGGER.info("Reusing existing Couchbase cluster on {}:8091", CouchbaseConfig.DEFAULT_HOSTNAME);
          } else {
            shared.start();
          }
        }
      }
    }
    return shared;
  }

  static GenericContainer<?> startDedicatedContainer() {
    return startDedicatedContainer(null, null);
  }

  static GenericContainer<?> startDedicatedContainer(Network network, String networkAlias) {
    releaseFixedPorts();
    GenericContainer<?> container = createDedicated();
    if (network != null) {
      container.withNetwork(network);
      if (networkAlias != null && !networkAlias.isBlank()) {
        container.withNetworkAliases(networkAlias);
      }
    }
    container.start();
    return container;
  }

  static GenericContainer<?> startCloudNativeGateway(
      Network network,
      String couchbaseHost,
      String username,
      String password) {
    stopDockerContainersOnPort(CNG_GRPC_PORT);
    stopDockerContainersOnPort(CNG_WEB_PORT);
    stopDockerContainersOnPort(CNG_DAPI_PORT);

    GenericContainer<?> cng = new GenericContainer<>(DockerImageName.parse(CNG_IMAGE))
        .withNetwork(network)
        .withExposedPorts(CNG_WEB_PORT, CNG_GRPC_PORT, CNG_DAPI_PORT)
        .withCommand(
            "--cb-host", couchbaseHost,
            "--cb-user", username,
            "--cb-pass", password,
            "--dapi-port", String.valueOf(CNG_DAPI_PORT),
            "--self-sign",
            "--daemon")
        .waitingFor(Wait.forHttp("/health")
            .forPort(CNG_WEB_PORT)
            .forStatusCode(200)
            .withStartupTimeout(Duration.ofMinutes(5)));

    List<String> bindings = List.of(
        CNG_WEB_PORT + ":" + CNG_WEB_PORT,
        CNG_GRPC_PORT + ":" + CNG_GRPC_PORT,
        CNG_DAPI_PORT + ":" + CNG_DAPI_PORT);
    cng.setPortBindings(bindings);
    cng.start();
    LOGGER.info("Cloud Native Gateway started on localhost:{}", CNG_GRPC_PORT);
    return cng;
  }

  static String containerNetworkIp(GenericContainer<?> container) {
    return container.getContainerInfo()
        .getNetworkSettings()
        .getNetworks()
        .values()
        .iterator()
        .next()
        .getIpAddress();
  }

  static void stopContainer(GenericContainer<?> container) {
    if (container == null) {
      return;
    }
    try {
      if (container.isRunning()) {
        container.stop();
      }
    } finally {
      container.close();
    }
  }

  static void releaseFixedPorts() {
    stopContainer(shared);
    shared = null;
    stopDockerContainersOnPort(8091);
    stopDockerContainersOnPort(CNG_GRPC_PORT);
    stopDockerContainersOnPort(CNG_WEB_PORT);
    stopDockerContainersOnPort(CNG_DAPI_PORT);
  }

  private static void stopDockerContainersOnPort(int port) {
    try {
      Process process = new ProcessBuilder("docker", "ps", "-q", "--filter", "publish=" + port)
          .redirectErrorStream(true)
          .start();
      String output = new String(process.getInputStream().readAllBytes()).trim();
      process.waitFor();
      if (output.isEmpty()) {
        return;
      }
      for (String containerId : output.split("\\R")) {
        if (containerId.isBlank()) {
          continue;
        }
        LOGGER.debug("Stopping container {} occupying port {}", containerId, port);
        new ProcessBuilder("docker", "rm", "-f", containerId).inheritIO().start().waitFor();
      }
    } catch (Exception e) {
      LOGGER.debug("Unable to stop containers on port {}: {}", port, e.getMessage());
    }
  }

  private static boolean clusterAlreadyRunning() {
    ClusterCreateSupport.ClusterRestEndpoint endpoint = ClusterCreateSupport.ClusterRestEndpoint.forServer(
        CouchbaseConfig.DEFAULT_HOSTNAME,
        false);
    if (!isPortListening(8091)) {
      return false;
    }
    if (!ClusterCreateSupport.isClusterInitialized(
        endpoint,
        CouchbaseConfig.DEFAULT_USER,
        CouchbaseConfig.DEFAULT_PASSWORD)) {
      return false;
    }
    return ClusterCreateSupport.isQueryReady(
        endpoint,
        CouchbaseConfig.DEFAULT_USER,
        CouchbaseConfig.DEFAULT_PASSWORD);
  }

  private static boolean isPortListening(int port) {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress(CouchbaseConfig.DEFAULT_HOSTNAME, port), 500);
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  static GenericContainer<?> createDedicated() {
    return configureContainer(new GenericContainer<>(DockerImageName.parse(IMAGE)));
  }

  static GenericContainer<?> createShared() {
    return configureContainer(new GenericContainer<>(DockerImageName.parse(IMAGE))
        .withReuse(true)
        .withCreateContainerCmdModifier(cmd -> cmd.withName(SHARED_CONTAINER_NAME)));
  }

  private static GenericContainer<?> configureContainer(GenericContainer<?> container) {
    container.withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
            .withMemory(4L * 1024 * 1024 * 1024))
        .waitingFor(Wait.forHttp("/ui/index.html")
            .forPort(8091)
            .withStartupTimeout(Duration.ofMinutes(10)));
    exposeFixedPorts(container);
    return container;
  }

  // Matches docker-compose port mappings:
  // 8091-8096, 9123, 11207, 11210, 11280, 18091-18097
  private static void exposeFixedPorts(GenericContainer<?> container) {
    int[] ports = {
        8091, 8092, 8093, 8094, 8095, 8096,
        9123,
        11207, 11210, 11280,
        18091, 18092, 18093, 18094, 18095, 18096, 18097
    };
    List<String> bindings = new ArrayList<>();
    for (int port : ports) {
      bindings.add(port + ":" + port);
    }
    container.setPortBindings(bindings);
    container.withExposedPorts(toPortArray(ports));
  }

  private static Integer[] toPortArray(int[] ports) {
    Integer[] boxed = new Integer[ports.length];
    for (int i = 0; i < ports.length; i++) {
      boxed[i] = ports[i];
    }
    return boxed;
  }
}
