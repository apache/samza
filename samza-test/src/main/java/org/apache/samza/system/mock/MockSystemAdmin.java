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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamPartition;

/**
 * A SystemAdmin that returns a constant set of partitions for all streams.
 */
public class MockSystemAdmin implements SystemAdmin {
  private final Set<Partition> partitions;

  public MockSystemAdmin(int partitionCount) {
    this.partitions = new HashSet<Partition>();

    for (int i = 0; i < partitionCount; ++i) {
      partitions.add(new Partition(i));
    }
  }

  @Override
  public Set<Partition> getPartitions(String streamName) {
    // Partitions are immutable, so making the set immutable should make the
    // partition set fully safe to re-use.
    return Collections.unmodifiableSet(partitions);
  }

  @Override
  public Map<SystemStreamPartition, String> getLastOffsets(Set<String> streams) {
    throw new RuntimeException("MockSystemAdmin doesn't implement this method.");
  }
}
