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

package org.apache.samza.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;

/**
 * A simple helper admin class that defines a single partition (partition 0) for
 * a given system. The metadata uses null for all offsets, which means that the
 * stream doesn't support offsets, and will be treated as empty. This class
 * should be used when a system has no concept of partitioning or offsets, since
 * Samza needs at least one partition for an input stream, in order to read it.
 */
public class SinglePartitionWithoutOffsetsSystemAdmin implements SystemAdmin {
  private static final Map<Partition, SystemStreamPartitionMetadata> FAKE_PARTITION_METADATA = new HashMap<Partition, SystemStreamPartitionMetadata>();

  static {
    FAKE_PARTITION_METADATA.put(new Partition(0), new SystemStreamPartitionMetadata(null, null, null));
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    Map<String, SystemStreamMetadata> metadata = new HashMap<String, SystemStreamMetadata>();

    for (String streamName : streamNames) {
      metadata.put(streamName, new SystemStreamMetadata(streamName, FAKE_PARTITION_METADATA));
    }

    return metadata;
  }

  @Override
  public void createChangelogStream(String streamName, int numOfPartitions) {
    throw new SamzaException("Method not implemented");
  }

  @Override
  public void validateChangelogStream(String streamName, int numOfPartitions) {
    throw new SamzaException("Method not implemented");
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
  public void createCoordinatorStream(String streamName) {
    throw new UnsupportedOperationException("Single partition admin can't create coordinator streams.");
  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    return null;
  }
}
