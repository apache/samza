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

package org.apache.samza.storage;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.StateCheckpointMarker;
import org.apache.samza.system.SystemStreamPartition;
import scala.Option;


public class StateCheckpointMarkers {
  private Map<SystemStreamPartition, StateCheckpointMarker> sspToStateCheckpointMarker;

  public StateCheckpointMarkers(Map<SystemStreamPartition, StateCheckpointMarker> mapping) {
    this.sspToStateCheckpointMarker = mapping;
  }

  public void mergeInto(Map<SystemStreamPartition, String> inputCheckpoints) {
    sspToStateCheckpointMarker.forEach((ssp, offset) -> {
      inputCheckpoints.put(ssp, offset.toString());
    });
  }

  public Map<SystemStreamPartition, Option<String>> toOffsets() {
    Map<SystemStreamPartition, Option<String>> sspToOffsets = new HashMap<>();
    sspToStateCheckpointMarker.forEach((ssp, scmOption) -> {
      String offset = scmOption.getChangelogOffset();
      Option<String> offsetOption = offset == null ? null : Option.apply(offset);
      sspToOffsets.put(ssp, offsetOption);
    });
    return sspToOffsets;
  }

  public static StateCheckpointMarkers fromOffsets(Map<SystemStreamPartition, Option<String>> sspToOffsets, CheckpointId id) {
    Map<SystemStreamPartition, StateCheckpointMarker> mapping = new HashMap<>();
    sspToOffsets.forEach((ssp, offsetOption) -> {
      StateCheckpointMarker marker = new StateCheckpointMarker(id, offsetOption.getOrElse(null));
      mapping.put(ssp, marker);
    });
    return new StateCheckpointMarkers(mapping);
  }
}
