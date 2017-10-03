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

package org.apache.samza.test.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;


/**
 * A dummy system admin
 */
public class SimpleSystemAdmin implements SystemAdmin {
  private final Config config;

  public SimpleSystemAdmin(Config config) {
    this.config = config;
  }

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    return offsets.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, null));
  }

  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    return streamNames.stream()
        .collect(Collectors.toMap(
            Function.<String>identity(),
            streamName -> {
            Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> metadataMap = new HashMap<>();
            int partitionCount = config.getInt("streams." + streamName + ".partitionCount", 1);
            for (int i = 0; i < partitionCount; i++) {
              metadataMap.put(new Partition(i), new SystemStreamMetadata.SystemStreamPartitionMetadata(null, null, null));
            }
            return new SystemStreamMetadata(streamName, metadataMap);
          }));
  }

  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    if (offset1 == null) {
      return offset2 == null ? 0 : -1;
    } else if (offset2 == null) {
      return 1;
    }
    return offset1.compareTo(offset2);
  }
}

