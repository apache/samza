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
package org.apache.samza.clustermanager;

import org.apache.samza.SamzaException;

/**
 * A fault domain is a set of hardware components that share a single point of failure.
 * This class identifies the type (ex: rack) and ID (ex: rack ID) of the fault domain in question.
 * A host can belong to multiple fault domains.
 * A fault domain may have greater than or equal to 1 hosts.
 * A cluster can comprise of hosts on multiple fault domains.
 */
public class FaultDomain {

  private final FaultDomainType type;
  private final String id;

  public FaultDomain(FaultDomainType type, String id) {
    if (type == null || id == null) {
      throw new SamzaException("Fault domain type and ID cannot be null.");
    }
    this.type = type;
    this.id = id;
  }

  /**
   * Gets the type of fault domain, for example: rack.
   * @return Type of fault domain
   */
  public FaultDomainType getType() {
    return type;
  }

  /**
   * Gets the id of the fault domain, for example: rack ID.
   * @return fault domain ID
   */
  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return " {" +
            "type = " + type +
            ", id = " + id +
            "} ";
  }

}
