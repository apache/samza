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

package org.apache.samza.container;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Used to represent the heartbeat response between
 * the JobCoordinator and the containers.
 * {@link ContainerHeartbeatResponse#isAlive()} is set to <code>true</code>
 * iff. the heartbeat is valid.
 */
public class ContainerHeartbeatResponse {

  private final boolean alive;

  public ContainerHeartbeatResponse(@JsonProperty("alive") boolean alive) {
    this.alive = alive;
  }

  public boolean isAlive() {
    return alive;
  }
}
