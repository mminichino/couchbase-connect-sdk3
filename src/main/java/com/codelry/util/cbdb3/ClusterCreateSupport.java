package com.codelry.util.cbdb3;

import com.codelry.util.capella.CapellaCluster;
import com.codelry.util.rest.REST;
import com.codelry.util.rest.exceptions.HttpResponseException;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ClusterCreateSupport {
  private static final List<String> DEFAULT_SERVER_SERVICES = List.of("data", "index", "query", "fts");
  private static final List<String> DEFAULT_CAPELLA_SERVICES = List.of("data", "query", "index", "search");
  private static final Pattern SERVER_NODE_PATTERN = Pattern.compile("^couchbase\\.server\\.(\\d+)\\.(.+)$");
  private static final Pattern CAPELLA_NODE_PATTERN = Pattern.compile("^capella\\.cluster\\.node\\.(\\d+)\\.(.+)$");

  private ClusterCreateSupport() {}

  static Map<String, String> mergeOptions(CouchbaseConfig config, Map<String, String> options) {
    Map<String, String> merged = new LinkedHashMap<>();
    Properties properties = config.getProperties();
    for (String key : properties.stringPropertyNames()) {
      merged.put(key, properties.getProperty(key));
    }
    if (options != null) {
      merged.putAll(options);
    }
    merged.putIfAbsent(CouchbaseConfig.COUCHBASE_USER, config.getUsername());
    merged.putIfAbsent(CouchbaseConfig.COUCHBASE_PASSWORD, config.getPassword());
    if (config.getHostname() != null) {
      merged.putIfAbsent(CouchbaseConfig.COUCHBASE_HOST, config.getHostname());
    }
    return merged;
  }

  static HostPort parseHostPort(String value, int defaultPort) {
    if (value == null || value.isBlank()) {
      return new HostPort(CouchbaseConfig.DEFAULT_HOSTNAME, defaultPort);
    }
    int slash = value.indexOf('/');
    if (slash >= 0) {
      value = value.substring(0, slash);
    }
    if (value.contains(":")) {
      String[] parts = value.split(":", 2);
      return new HostPort(parts[0], Integer.parseInt(parts[1]));
    }
    return new HostPort(value, defaultPort);
  }

  static List<ClusterNodeConfig> parseServerNodes(Map<String, String> options) {
    Map<Integer, ClusterNodeConfig> nodes = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      Matcher matcher = SERVER_NODE_PATTERN.matcher(entry.getKey());
      if (!matcher.matches()) {
        continue;
      }
      int index = Integer.parseInt(matcher.group(1));
      String field = matcher.group(2);
      ClusterNodeConfig node = nodes.computeIfAbsent(index, ignored -> new ClusterNodeConfig());
      switch (field) {
        case "ip" -> node.setIp(entry.getValue());
        case "ram" -> node.setRamGiB(Integer.parseInt(entry.getValue()));
        case "services" -> node.setServices(parseServices(entry.getValue(), DEFAULT_SERVER_SERVICES));
        default -> {
        }
      }
    }
    if (nodes.isEmpty()) {
      ClusterNodeConfig node = new ClusterNodeConfig()
          .setIp(options.getOrDefault(CouchbaseConfig.COUCHBASE_HOST, CouchbaseConfig.DEFAULT_HOSTNAME))
          .setServices(DEFAULT_SERVER_SERVICES);
      nodes.put(0, node);
    }
    return new ArrayList<>(nodes.values());
  }

  static List<CapellaNodeConfig> parseCapellaNodes(Map<String, String> options) {
    Map<Integer, CapellaNodeConfig> nodes = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      Matcher matcher = CAPELLA_NODE_PATTERN.matcher(entry.getKey());
      if (!matcher.matches()) {
        continue;
      }
      int index = Integer.parseInt(matcher.group(1));
      String field = matcher.group(2);
      CapellaNodeConfig node = nodes.computeIfAbsent(index, ignored -> new CapellaNodeConfig());
      switch (field) {
        case "cpu" -> node.setCpu(Integer.parseInt(entry.getValue()));
        case "ram" -> node.setRam(Integer.parseInt(entry.getValue()));
        case "services" -> node.setServices(parseCapellaServices(entry.getValue()));
        default -> {
        }
      }
    }
    return new ArrayList<>(nodes.values());
  }

  static CapellaCluster.ClusterConfig buildCapellaClusterConfig(Map<String, String> options, List<CapellaNodeConfig> nodes) {
    CapellaCluster.ClusterConfig config = new CapellaCluster.ClusterConfig();
    if (nodes.isEmpty()) {
      return config.singleNode(DEFAULT_CAPELLA_SERVICES);
    }
    config.availability(com.codelry.util.capella.logic.AvailabilityType.SINGLE_ZONE);
    for (CapellaNodeConfig node : nodes) {
      config.addServiceGroup(new CapellaCluster.ServiceGroupConfig()
          .cpu(node.getCpu())
          .ram(node.getRam())
          .numOfNodes(1)
          .storage(100)
          .services(node.getServices()));
    }
    return config;
  }

  static Map<String, Integer> calculateServerQuotas(ClusterNodeConfig node, Map<String, String> options) {
    int availableMiB = (int) Math.floor(node.getRamGiB() * 1024 * 0.8);
    long quotaServiceCount = node.getServices().stream()
        .filter(service -> !isQueryService(service))
        .count();
    if (quotaServiceCount == 0) {
      quotaServiceCount = 1;
    }
    int defaultQuota = Math.max(256, availableMiB / (int) quotaServiceCount);
    Map<String, Integer> quotas = new HashMap<>();
    for (String service : node.getServices()) {
      if (isQueryService(service)) {
        continue;
      }
      String propertyKey = CouchbaseConfig.serverQuotaKey(service);
      quotas.put(normalizeServerService(service), readQuotaOverride(options, propertyKey, defaultQuota));
    }
    return quotas;
  }

  static String toRestServices(List<String> services) {
    List<String> restServices = new ArrayList<>();
    for (String service : services) {
      restServices.add(toRestService(service));
    }
    return String.join(",", restServices);
  }

  static void initializeSingleNodeCluster(
      ClusterRestEndpoint endpoint,
      String username,
      String password,
      List<String> services,
      Map<String, Integer> quotas) {
    String clusterHostname = clusterInitHostname(endpoint.host());
    Map<String, String> fields = new LinkedHashMap<>();
    fields.put("hostname", clusterHostname);
    fields.put("username", username);
    fields.put("password", password);
    fields.put("port", "SAME");
    fields.put("services", toRestServices(services));
    fields.put("allowedHosts", allowedHosts(clusterHostname));
    fields.put("indexerStorageMode", "plasma");
    applyQuotaFields(fields, quotas);
    postForm(endpoint, null, null, "clusterInit", fields);
  }

  static String allowedHosts(String clusterHostname) {
    return clusterHostname;
  }

  static String clusterInitHostname(String host) {
    if (host == null || host.isBlank()) {
      return "127.0.0.1";
    }
    String normalized = host.toLowerCase();
    if ("localhost".equals(normalized)) {
      return "127.0.0.1";
    }
    if (normalized.contains(".")) {
      return host;
    }
    return host + ".local";
  }

  static void addNodeToCluster(
      ClusterRestEndpoint endpoint,
      String username,
      String password,
      String nodeHost,
      List<String> services) {
    Map<String, String> fields = new LinkedHashMap<>();
    fields.put("hostname", nodeHost);
    fields.put("user", username);
    fields.put("password", password);
    fields.put("services", toRestServices(services));
    postForm(endpoint, username, password, "controller/addNode", fields);
  }

  static void rebalanceCluster(
      ClusterRestEndpoint endpoint,
      String username,
      String password,
      List<String> nodeHosts) {
    List<String> knownNodes = new ArrayList<>();
    for (String nodeHost : nodeHosts) {
      knownNodes.add("ns_1@" + nodeHost);
    }
    Map<String, String> fields = new LinkedHashMap<>();
    fields.put("knownNodes", String.join(",", knownNodes));
    postForm(endpoint, username, password, "controller/rebalance", fields);
  }

  static void waitForCluster(ClusterRestEndpoint endpoint, String username, String password, int retries) {
    REST rest = adminClient(endpoint, username, password);
    for (int attempt = 0; attempt < retries; attempt++) {
      try {
        JsonNode response = rest.get("pools/default").validate().json();
        if (response.has("nodes") && response.get("nodes").size() > 0) {
          return;
        }
      } catch (Exception ignored) {
      }
      sleep(Duration.ofSeconds(2));
    }
    throw new RuntimeException("Timed out waiting for Couchbase cluster at "
        + endpoint.host() + ":" + endpoint.adminPort());
  }

  static void waitForRebalanceComplete(ClusterRestEndpoint endpoint, String username, String password) {
    REST rest = adminClient(endpoint, username, password);
    for (int attempt = 0; attempt < 120; attempt++) {
      try {
        if (!isRebalanceInProgress(rest)) {
          return;
        }
      } catch (Exception ignored) {
      }
      sleep(Duration.ofSeconds(2));
    }
    throw new RuntimeException("Timed out waiting for rebalance to complete at "
        + endpoint.host() + ":" + endpoint.adminPort());
  }

  private static boolean isRebalanceInProgress(REST rest) {
    try {
      JsonNode progress = rest.get("pools/default/rebalanceProgress").validate().json();
      if (progress.has("status") && !progress.get("status").isNull()) {
        String status = progress.get("status").asText();
        if ("running".equalsIgnoreCase(status)) {
          return true;
        }
        if ("none".equalsIgnoreCase(status)) {
          return false;
        }
      }

      JsonNode pools = rest.get("pools/default").validate().json();
      if (pools.has("rebalanceStatus") && !pools.get("rebalanceStatus").isNull()) {
        String status = pools.get("rebalanceStatus").asText();
        if (!status.isBlank() && !"none".equalsIgnoreCase(status)) {
          return true;
        }
      }

      JsonNode tasks = rest.get("pools/default/tasks").validate().json();
      if (tasks.isArray()) {
        for (JsonNode task : tasks) {
          if ("rebalance".equals(task.path("type").asText())
              && "running".equalsIgnoreCase(task.path("status").asText())) {
            return true;
          }
        }
      }
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void waitForQueryReady(ClusterRestEndpoint endpoint, String username, String password) {
    if (isQueryReady(endpoint, username, password)) {
      return;
    }
    for (int attempt = 0; attempt < 60; attempt++) {
      sleep(Duration.ofSeconds(2));
      if (isQueryReady(endpoint, username, password)) {
        return;
      }
    }
    throw new RuntimeException("Timed out waiting for query service at "
        + endpoint.host() + ":" + endpoint.queryPort());
  }

  static boolean isQueryReady(ClusterRestEndpoint endpoint, String username, String password) {
    try {
      formClient(endpoint, username, password, endpoint.queryPort())
          .post("query/service", Map.of("statement", "SELECT 1"))
          .validate();
      return true;
    } catch (Exception ignored) {
      return false;
    }
  }

  static void waitForClusterServices(ClusterRestEndpoint endpoint, String username, String password) {
    REST rest = adminClient(endpoint, username, password);
    for (int attempt = 0; attempt < 120; attempt++) {
      try {
        JsonNode response = rest.get("pools/default").validate().json();
        if (servicesRunning(response, "kv", "n1ql", "index")) {
          return;
        }
      } catch (Exception ignored) {
      }
      sleep(Duration.ofSeconds(2));
    }
    throw new RuntimeException("Timed out waiting for Couchbase cluster services at "
        + endpoint.host() + ":" + endpoint.adminPort());
  }

  static boolean isClusterInitialized(ClusterRestEndpoint endpoint, String username, String password) {
    REST rest = adminClient(endpoint, username, password);
    try {
      JsonNode response = rest.get("pools/default").validate().json();
      return servicesRunning(response, "kv", "n1ql", "index");
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean servicesRunning(JsonNode pools, String... restServices) {
    if (!pools.has("nodes") || pools.get("nodes").isEmpty()) {
      return false;
    }
    JsonNode services = pools.get("nodes").get(0).get("services");
    if (services == null || services.isNull()) {
      return false;
    }
    for (String service : restServices) {
      if (!serviceEnabled(services, service)) {
        return false;
      }
    }
    return true;
  }

  private static boolean serviceEnabled(JsonNode services, String service) {
    if (services.isArray()) {
      for (JsonNode entry : services) {
        if (service.equals(entry.asText())) {
          return true;
        }
      }
      return false;
    }
    if (!services.has(service)) {
      return false;
    }
    String value = services.get(service).asText();
    return value != null && !value.isBlank() && !"notRunning".equalsIgnoreCase(value);
  }

  static void postForm(
      ClusterRestEndpoint endpoint,
      String username,
      String password,
      String path,
      Map<String, String> fields) {
    String normalizedEndpoint = path.replaceAll("^/+", "");
    REST rest = formClient(endpoint, username, password, endpoint.adminPort());
    try {
      rest.post(normalizedEndpoint, fields).validate();
    } catch (HttpResponseException e) {
      throw new RuntimeException("HTTP " + rest.code() + " from " + path + ": " + e.getMessage(), e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to POST form data to " + path, e);
    }
  }

  private static void applyQuotaFields(Map<String, String> fields, Map<String, Integer> quotas) {
    if (quotas.containsKey("data")) {
      fields.put("memoryQuota", String.valueOf(quotas.get("data")));
    }
    if (quotas.containsKey("index")) {
      fields.put("indexMemoryQuota", String.valueOf(quotas.get("index")));
    }
    if (quotas.containsKey("fts")) {
      fields.put("ftsMemoryQuota", String.valueOf(quotas.get("fts")));
    }
    if (quotas.containsKey("eventing")) {
      fields.put("eventingMemoryQuota", String.valueOf(quotas.get("eventing")));
    }
    if (quotas.containsKey("analytics")) {
      fields.put("cbasMemoryQuota", String.valueOf(quotas.get("analytics")));
    }
  }

  private static int readQuotaOverride(Map<String, String> options, String propertyKey, int defaultQuota) {
    String value = options.get(propertyKey);
    if (value == null || value.isBlank()) {
      return defaultQuota;
    }
    return Integer.parseInt(value);
  }

  private static List<String> parseServices(String value, List<String> defaultServices) {
    if (value == null || value.isBlank()) {
      return new ArrayList<>(defaultServices);
    }
    List<String> services = new ArrayList<>();
    for (String service : value.split(",")) {
      services.add(normalizeServerService(service.trim()));
    }
    return services;
  }

  private static List<String> parseCapellaServices(String value) {
    if (value == null || value.isBlank()) {
      return new ArrayList<>(DEFAULT_CAPELLA_SERVICES);
    }
    List<String> services = new ArrayList<>();
    for (String service : value.split(",")) {
      services.add(normalizeCapellaService(service.trim()));
    }
    return services;
  }

  private static String normalizeServerService(String service) {
    return switch (service.toLowerCase()) {
      case "kv", "data" -> "data";
      case "n1ql", "query" -> "query";
      case "index" -> "index";
      case "fts", "search" -> "fts";
      case "eventing" -> "eventing";
      case "cbas", "analytics" -> "analytics";
      default -> service.toLowerCase();
    };
  }

  private static String normalizeCapellaService(String service) {
    return switch (service.toLowerCase()) {
      case "kv", "data" -> "data";
      case "n1ql", "query" -> "query";
      case "index" -> "index";
      case "fts", "search" -> "search";
      case "eventing" -> "eventing";
      case "cbas", "analytics" -> "analytics";
      default -> service.toLowerCase();
    };
  }

  private static String toRestService(String service) {
    return switch (normalizeServerService(service)) {
      case "data" -> "kv";
      case "query" -> "n1ql";
      case "index" -> "index";
      case "fts" -> "fts";
      case "eventing" -> "eventing";
      case "analytics" -> "cbas";
      default -> service;
    };
  }

  private static boolean isQueryService(String service) {
    String normalized = normalizeServerService(service);
    return "query".equals(normalized);
  }

  private static void sleep(Duration duration) {
    try {
      Thread.sleep(duration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  record HostPort(String host, int port) {}

  record ClusterRestEndpoint(String host, int adminPort, int queryPort, boolean useSsl) {
    static ClusterRestEndpoint forServer(String host, boolean useSsl) {
      return new ClusterRestEndpoint(host, useSsl ? 18091 : 8091, useSsl ? 18093 : 8093, useSsl);
    }

    static ClusterRestEndpoint forCapella(String host) {
      return new ClusterRestEndpoint(host, 18091, 18093, true);
    }
  }

  private static REST adminClient(ClusterRestEndpoint endpoint, String username, String password) {
    return new REST(endpoint.host(), username, password, endpoint.useSsl(), endpoint.adminPort()).enableDebug(false);
  }

  private static REST formClient(
      ClusterRestEndpoint endpoint,
      String username,
      String password,
      int port) {
    return new REST(endpoint.host(), username, password, endpoint.useSsl(), port).enableDebug(false);
  }
}
