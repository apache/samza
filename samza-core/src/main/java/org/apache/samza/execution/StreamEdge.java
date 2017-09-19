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

import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Util;


/**
 * A StreamEdge connects the source {@link JobNode}s to the target {@link JobNode}s with a stream.
 * If it's a sink StreamEdge, the target JobNode is empty.
 * If it's a source StreamEdge, the source JobNode is empty.
 */
public class StreamEdge {
  public static final int PARTITIONS_UNKNOWN = -1;

  private final StreamSpec streamSpec;
  private final List<JobNode> sourceNodes = new ArrayList<>();
  private final List<JobNode> targetNodes = new ArrayList<>();
  private final Config config;

  private String name = "";
  private int partitions = PARTITIONS_UNKNOWN;
  private final boolean isIntermediate;

  StreamEdge(StreamSpec streamSpec, Config config) {
    this(streamSpec, false, config);
  }

  StreamEdge(StreamSpec streamSpec, boolean isIntermediate, Config config) {
    this.streamSpec = streamSpec;
    this.name = Util.getNameFromSystemStream(getSystemStream());
    this.isIntermediate = isIntermediate;
    this.config = config;
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

    if (isIntermediate) {
      ApplicationConfig appConfig = new ApplicationConfig(config);
      if (appConfig.getAppMode() == ApplicationConfig.ApplicationMode.BATCH && appConfig.getRunId() != null) {
        spec = spec.copyWithPhysicalName(spec.getPhysicalName() + "-" + appConfig.getRunId());
      }
    }
    return spec;
  }

  SystemStream getSystemStream() {
    return new SystemStream(streamSpec.getSystemName(), streamSpec.getPhysicalName());
  }

  String getFormattedSystemStream() {
    return Util.getNameFromSystemStream(getSystemStream());
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

  void setName(String name) {
    this.name = name;
  }

  boolean isIntermediate() {
    return isIntermediate;
  }

  Config generateConfig() {
    Map<String, String> config = new HashMap<>();
    StreamSpec spec = getStreamSpec();
    config.put(String.format(StreamConfig.SYSTEM_FOR_STREAM_ID(), spec.getId()), spec.getSystemName());
    config.put(String.format(StreamConfig.PHYSICAL_NAME_FOR_STREAM_ID(), spec.getId()), spec.getPhysicalName());
    if (isIntermediate()) {
      config.put(String.format(StreamConfig.IS_INTERMEDIATE_FOR_STREAM_ID(), spec.getId()), "true");
      config.put(String.format(StreamConfig.CONSUMER_OFFSET_DEFAULT_FOR_STREAM_ID(), spec.getId()), "oldest");
    }
    if (spec.isBounded()) {
      config.put(String.format(StreamConfig.IS_BOUNDED_FOR_STREAM_ID(), spec.getId()), "true");
    }
    spec.getConfig().forEach((property, value) -> {
      config.put(String.format(StreamConfig.STREAM_ID_PREFIX(), spec.getId()) + property, value);
    });
    return new MapConfig(config);
  }
}
