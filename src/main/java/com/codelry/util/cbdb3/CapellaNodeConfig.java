package com.codelry.util.cbdb3;

import java.util.ArrayList;
import java.util.List;

/**
 * Describes a Capella service group node configuration used during cluster creation.
 */
public class CapellaNodeConfig {
  private int cpu = 4;
  private int ram = 16;
  private List<String> services = new ArrayList<>(List.of("data", "query", "index", "search"));

  public int getCpu() {
    return cpu;
  }

  public CapellaNodeConfig setCpu(int cpu) {
    this.cpu = cpu;
    return this;
  }

  public int getRam() {
    return ram;
  }

  public CapellaNodeConfig setRam(int ram) {
    this.ram = ram;
    return this;
  }

  public List<String> getServices() {
    return services;
  }

  public CapellaNodeConfig setServices(List<String> services) {
    this.services = services;
    return this;
  }
}
