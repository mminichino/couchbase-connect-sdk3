package com.codelry.util.cbdb3;

import java.util.List;

public class GroupData {
  private String id;
  private String description;
  private List<RoleData> roles;

  public String getId() {
    return id;
  }

  public String getDescription() {
    return description;
  }

  public List<RoleData> getRoles() {
    return roles;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void setRoles(List<RoleData> roles) {
    this.roles = roles;
  }
}
