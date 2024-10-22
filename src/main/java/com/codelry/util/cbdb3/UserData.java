package com.codelry.util.cbdb3;

import java.util.List;

public class UserData {
  private String id;
  private String password;
  private String name;
  private String email;
  private List<String> groups;
  private List<RoleData> roles;

  public String getId() {
    return id;
  }

  public String getPassword() {
    return password;
  }

  public String getName() {
    return name;
  }

  public String getEmail() {
    return email;
  }

  public List<String> getGroups() {
    return groups;
  }

  public List<RoleData> getRoles() {
    return roles;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public void setGroups(List<String> groups) {
    this.groups = groups;
  }

  public void setRoles(List<RoleData> roles) {
    this.roles = roles;
  }
}
