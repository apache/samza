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

import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;


public class MockStateCheckpointPayloadSerde implements StateCheckpointPayloadSerde<MockStateCheckpointMarker> {
  private static final String SEPARATOR = ";";

  @Override
  public String serialize(MockStateCheckpointMarker payload) {
    return payload.ssp.getSystem() + SEPARATOR + payload.ssp.getStream() + SEPARATOR + payload.ssp.getPartition()
        .getPartitionId() + SEPARATOR + payload.offset;
  }

  @Override
  public MockStateCheckpointMarker deserialize(String data) {
    String[] parts = data.split(SEPARATOR);
    return new MockStateCheckpointMarker(
        new SystemStreamPartition(parts[0], parts[1], new Partition(Integer.parseInt(parts[2]))),
        parts[3]);
  }
}
