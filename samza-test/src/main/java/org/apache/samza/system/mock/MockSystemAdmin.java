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

package org.apache.samza.system.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;

/**
 * A SystemAdmin that returns a constant set of partitions for all streams.
 */
public class MockSystemAdmin implements SystemAdmin {
  private final Map<Partition, SystemStreamPartitionMetadata> systemStreamPartitionMetadata;

  public MockSystemAdmin(int partitionCount) {
    this.systemStreamPartitionMetadata = new HashMap<Partition, SystemStreamPartitionMetadata>();

    for (int i = 0; i < partitionCount; ++i) {
      systemStreamPartitionMetadata.put(new Partition(i), new SystemStreamPartitionMetadata(null, null, null));
    }
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    Map<String, SystemStreamMetadata> metadata = new HashMap<String, SystemStreamMetadata>();

    for (String streamName : streamNames) {
      metadata.put(streamName, new SystemStreamMetadata(streamName, systemStreamPartitionMetadata));
    }

    return metadata;
  }

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    Map<SystemStreamPartition, String> offsetsAfter = new HashMap<SystemStreamPartition, String>();

    for (SystemStreamPartition systemStreamPartition : offsets.keySet()) {
      offsetsAfter.put(systemStreamPartition, null);
    }

    return offsetsAfter;
  }

  @Override
  public void createChangelogStream(String streamName, int numOfPartitions) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void validateChangelogStream(String streamName, int numOfPartitions) {
    throw new UnsupportedOperationException("Method not implemented");
  }

  @Override
  public void createCoordinatorStream(String streamName) {
    throw new UnsupportedOperationException("Method not implemented.");
  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    return null;
  }
}