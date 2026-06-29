package com.codelry.util.cbdb3;

import java.util.ArrayList;
import java.util.List;

/**
 * Describes a Couchbase Server node used during cluster creation.
 */
public class ClusterNodeConfig {
  private String ip;
  private int ramGiB = 8;
  private List<String> services = new ArrayList<>(List.of("data", "index", "query", "fts"));

  public String getIp() {
    return ip;
  }

  public ClusterNodeConfig setIp(String ip) {
    this.ip = ip;
    return this;
  }

  public int getRamGiB() {
    return ramGiB;
  }

  public ClusterNodeConfig setRamGiB(int ramGiB) {
    this.ramGiB = ramGiB;
    return this;
  }

  public List<String> getServices() {
    return services;
  }

  public ClusterNodeConfig setServices(List<String> services) {
    this.services = services;
    return this;
  }
}
