/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.samza.table.remote.couchbase;

import java.io.Serializable;


public class CouchbaseEnvironmentConfigs implements Serializable {
  public CouchbaseEnvironmentConfigs() {
  }

  protected Boolean sslEnabled;
  protected Boolean certAuthEnabled;
  protected String sslKeystoreFile;
  protected String sslKeystorePassword;
  protected String sslTruststoreFile;
  protected String sslTruststorePassword;
  protected Integer bootstrapCarrierDirectPort;
  protected Integer bootstrapCarrierSslPort;
  protected Integer bootstrapHttpDirectPort;
  protected Integer bootstrapHttpSslPort;
  protected String username;
  protected String password;

  public Boolean getSslEnabled() {
    return sslEnabled;
  }

  public void setSslEnabled(Boolean sslEnabled) {
    this.sslEnabled = sslEnabled;
  }

  public Boolean getCertAuthEnabled() {
    return certAuthEnabled;
  }

  public void setCertAuthEnabled(Boolean certAuthEnabled) {
    this.certAuthEnabled = certAuthEnabled;
  }

  public String getSslKeystoreFile() {
    return sslKeystoreFile;
  }

  public void setSslKeystoreFile(String sslKeystoreFile) {
    this.sslKeystoreFile = sslKeystoreFile;
  }

  public String getSslKeystorePassword() {
    return sslKeystorePassword;
  }

  public void setSslKeystorePassword(String sslKeystorePassword) {
    this.sslKeystorePassword = sslKeystorePassword;
  }

  public String getSslTruststoreFile() {
    return sslTruststoreFile;
  }

  public void setSslTruststoreFile(String sslTruststoreFile) {
    this.sslTruststoreFile = sslTruststoreFile;
  }

  public String getSslTruststorePassword() {
    return sslTruststorePassword;
  }

  public void setSslTruststorePassword(String sslTruststorePassword) {
    this.sslTruststorePassword = sslTruststorePassword;
  }

  public Integer getBootstrapCarrierDirectPort() {
    return bootstrapCarrierDirectPort;
  }

  public void setBootstrapCarrierDirectPort(Integer bootstrapCarrierDirectPort) {
    this.bootstrapCarrierDirectPort = bootstrapCarrierDirectPort;
  }

  public Integer getBootstrapCarrierSslPort() {
    return bootstrapCarrierSslPort;
  }

  public void setBootstrapCarrierSslPort(Integer bootstrapCarrierSslPort) {
    this.bootstrapCarrierSslPort = bootstrapCarrierSslPort;
  }

  public Integer getBootstrapHttpDirectPort() {
    return bootstrapHttpDirectPort;
  }

  public void setBootstrapHttpDirectPort(Integer bootstrapHttpDirectPort) {
    this.bootstrapHttpDirectPort = bootstrapHttpDirectPort;
  }

  public Integer getBootstrapHttpSslPort() {
    return bootstrapHttpSslPort;
  }

  public void setBootstrapHttpSslPort(Integer bootstrapHttpSslPort) {
    this.bootstrapHttpSslPort = bootstrapHttpSslPort;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
