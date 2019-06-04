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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.StreamUtil;


/**
 * A StreamEdge connects the source {@link JobNode}s to the target {@link JobNode}s with a stream.
 * If it's a sink StreamEdge, the target JobNode is empty.
 * If it's a source StreamEdge, the source JobNode is empty.
 */
public class StreamEdge {
  public static final int PARTITIONS_UNKNOWN = -1;

  private final StreamSpec streamSpec;
  private final boolean isBroadcast;
  private final boolean isIntermediate;
  private final List<JobNode> sourceNodes = new ArrayList<>();
  private final List<JobNode> targetNodes = new ArrayList<>();
  private final Config config;
  private final String name;

  private int partitions = PARTITIONS_UNKNOWN;

  StreamEdge(StreamSpec streamSpec, boolean isIntermediate, boolean isBroadcast, Config config) {
    this.streamSpec = streamSpec;
    this.isIntermediate = isIntermediate;
    // broadcast can be configured either by an operator or via the configs
    this.isBroadcast =
          isBroadcast || new StreamConfig(config).getBroadcastEnabled(streamSpec.toSystemStream());
    this.config = config;
    if (isBroadcast && isIntermediate) {
      partitions = 1;
    }
    this.name = StreamUtil.getNameFromSystemStream(getSystemStream());
  }

  void addSourceNode(JobNode sourceNode) {
    sourceNodes.add(sourceNode);
  }

  void addTargetNode(JobNode targetNode) {
    targetNodes.add(targetNode);
  }

  StreamSpec getStreamSpec() {
    StreamSpec spec = (partitions == PARTITIONS_UNKNOWN) ?
        streamSpec : streamSpec.copyWithPartitionCount(partitions);

    // For intermediate stream that physical name is the same as id,
    // meaning the physical name is auto-generated, and not overrided
    // by user or batch processing.
    if (isIntermediate && spec.getId().equals(spec.getPhysicalName())) {
      // Append unique id to the batch intermediate streams
      // Note this will only happen for batch processing
      String physicalName = StreamManager.createUniqueNameForBatch(spec.getPhysicalName(), config);
      if (!physicalName.equals(spec.getPhysicalName())) {
        spec = spec.copyWithPhysicalName(physicalName);
      }
    }
    return spec;
  }

  SystemStream getSystemStream() {
    return getStreamSpec().toSystemStream();
  }

  List<JobNode> getSourceNodes() {
    return sourceNodes;
  }

  List<JobNode> getTargetNodes() {
    return targetNodes;
  }

  int getPartitionCount() {
    return partitions;
  }

  void setPartitionCount(int partitions) {
    this.partitions = partitions;
  }

  String getName() {
    return name;
  }

  boolean isIntermediate() {
    return isIntermediate;
  }

  Config generateConfig() {
    Map<String, String> streamConfig = new HashMap<>();
    StreamSpec spec = getStreamSpec();
    streamConfig.put(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), spec.getId()), spec.getSystemName());
    streamConfig.put(String.format(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID(), spec.getId()), spec.getPhysicalName());
    if (isIntermediate()) {
      streamConfig.put(String.format(StreamConfig.IS_INTERMEDIATE_FOR_STREAM_ID(), spec.getId()), "true");
      streamConfig.put(String.format(StreamConfig.DELETE_COMMITTED_MESSAGES_FOR_STREAM_ID(), spec.getId()), "true");
      streamConfig.put(String.format(StreamConfig.CONSUMER_OFFSET_DEFAULT_FOR_STREAM_ID(), spec.getId()), "oldest");
      streamConfig.put(String.format(StreamConfig.PRIORITY_FOR_STREAM_ID(), spec.getId()), String.valueOf(Integer.MAX_VALUE));
    }
    spec.getConfig().forEach((property, value) -> {
        streamConfig.put(String.format(StreamConfig.STREAM_ID_PREFIX(), spec.getId()) + property, value);
      });

    return new MapConfig(streamConfig);
  }

  public boolean isBroadcast() {
    return isBroadcast;
  }
}
