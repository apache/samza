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
package org.apache.samza.execution;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamManager.class);

  private final Map<String, SystemAdmin> sysAdmins;

  public StreamManager(Map<String, SystemAdmin> sysAdmins) {
    this.sysAdmins = sysAdmins;
  }

  public void createStreams(List<StreamSpec> streams) {
    Multimap<String, StreamSpec> streamsGroupedBySystem = HashMultimap.create();
    streams.forEach(streamSpec ->
      streamsGroupedBySystem.put(streamSpec.getSystemName(), streamSpec));

    for (Map.Entry<String, Collection<StreamSpec>> entry : streamsGroupedBySystem.asMap().entrySet()) {
      String systemName = entry.getKey();
      SystemAdmin systemAdmin = sysAdmins.get(systemName);

      for (StreamSpec stream : entry.getValue()) {
        LOGGER.info("Creating stream {} with partitions {} on system {}",
            new Object[]{stream.getPhysicalName(), stream.getPartitionCount(), systemName});
        systemAdmin.createStream(stream);
      }
    }
  }

  Map<String, Integer> getStreamPartitionCounts(String systemName, Set<String> streamNames) {
    Map<String, Integer> streamToPartitionCount = new HashMap<>();

    SystemAdmin systemAdmin = sysAdmins.get(systemName);
    if (systemAdmin == null) {
      throw new SamzaException(String.format("System %s does not exist.", systemName));
    }

    // retrieve the metadata for the streams in this system
    Map<String, SystemStreamMetadata> streamToMetadata = systemAdmin.getSystemStreamMetadata(streamNames);
    // set the partitions of a stream to its StreamEdge
    streamToMetadata.forEach((stream, data) ->
      streamToPartitionCount.put(stream, data.getSystemStreamPartitionMetadata().size()));

    return streamToPartitionCount;
  }

  /**
   * Check if the streams described by the specs already exist.
   * @param streams A list of stream specs, whose existence we need to check for
   * @return true if all the streams exist already, false otherwise
   */
  public boolean checkIfStreamsExist(List<StreamSpec> streams) {
    for (StreamSpec spec: streams) {
      SystemAdmin systemAdmin = sysAdmins.get(spec.getSystemName());
      if (!systemAdmin.existStream(spec)) {
        return false;
      }
    }
    return true;
  }
}
