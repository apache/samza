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

package org.apache.samza.job.model;

import com.google.common.base.Objects;

/**
 * A class to represent the host locality information.
 * Fields such as <i>jmxUrl</i> and <i>jmxTunnelingUrl</i> exist for backward compatibility reasons as they were
 * historically stored under the same name space as locality and surfaced within the framework through the locality
 * manager.
 */
public class HostLocality {
  /* Container identifier */
  private String id;
  /* Host on which the container is currently placed */
  private String host;
  private String jmxUrl;
  /* JMX tunneling URL for debugging */
  private String jmxTunnelingUrl;

  public HostLocality(String id, String host) {
    this(id, host, "", "");
  }

  public HostLocality(String id, String host, String jmxUrl, String jmxTunnelingUrl) {
    this.id = id;
    this.host = host;
    this.jmxUrl = jmxUrl;
    this.jmxTunnelingUrl = jmxTunnelingUrl;
  }

  public String id() {
    return id;
  }

  public String host() {
    return host;
  }

  public String jmxUrl() {
    return jmxUrl;
  }

  public String jmxTunnelingUrl() {
    return jmxTunnelingUrl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HostLocality that = (HostLocality) o;
    return Objects.equal(id, that.id)
        && Objects.equal(host, that.host)
        && Objects.equal(jmxUrl, that.jmxUrl)
        && Objects.equal(jmxTunnelingUrl, that.jmxTunnelingUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, host, jmxUrl, jmxTunnelingUrl);
  }
}
