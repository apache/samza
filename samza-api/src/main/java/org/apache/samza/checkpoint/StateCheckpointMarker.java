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

package org.apache.samza.checkpoint;

import com.google.common.base.Objects;

// TODO HIGH dchen add javadocs explaining what this class represents and how it's used
public class StateCheckpointMarker {
  private final String stateBackendFactoryName;
  private final String stateCheckpointMarker;

  public StateCheckpointMarker(String stateBackendFactoryName, String stateCheckpointMarker) {
    this.stateBackendFactoryName = stateBackendFactoryName;
    this.stateCheckpointMarker = stateCheckpointMarker;
  }

  public String getStateBackendFactoryName() {
    return stateBackendFactoryName;
  }

  public String getStateCheckpointMarker() {
    return stateCheckpointMarker;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StateCheckpointMarker that = (StateCheckpointMarker) o;

    return Objects.equal(getStateBackendFactoryName(), that.getStateBackendFactoryName()) &&
        Objects.equal(getStateCheckpointMarker(), that.getStateCheckpointMarker());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getStateBackendFactoryName(), getStateCheckpointMarker());
  }
}
