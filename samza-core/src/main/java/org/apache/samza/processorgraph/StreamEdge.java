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

package org.apache.samza.processorgraph;

import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.samza.config.Config;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Util;


/**
 * A StreamEdge connects the source {@link ProcessorNode}s to the target {@link ProcessorNode}s with a stream.
 * If it's a sink StreamEdge, the target ProcessorNode is empty.
 * If it's a source StreamEdge, the source ProcessorNode is empty.
 */
public class StreamEdge {
  private final StreamSpec streamSpec;
  private final List<ProcessorNode> sourceNodes = new ArrayList<>();
  private final List<ProcessorNode> targetNodes = new ArrayList<>();
  private final Config config;

  private String name = "";
  private int partitions = -1;

  StreamEdge(StreamSpec streamSpec, Config config) {
    this.streamSpec = streamSpec;
    this.config = config;
    this.name = Util.getNameFromSystemStream(getSystemStream());
  }

  void addSourceNode(ProcessorNode sourceNode) {
    sourceNodes.add(sourceNode);
  }

  void addTargetNode(ProcessorNode targetNode) {
    targetNodes.add(targetNode);
  }

  StreamSpec getStreamSpec() {
    return streamSpec;
  }

  SystemStream getSystemStream() {
    return new SystemStream(streamSpec.getSystemName(), streamSpec.getPhysicalName());
  }

  String getFormattedSystemStream() {
    return Util.getNameFromSystemStream(getSystemStream());
  }

  List<ProcessorNode> getSourceNodes() {
    return sourceNodes;
  }

  List<ProcessorNode> getTargetNodes() {
    return targetNodes;
  }

  int getPartitions() {
    return partitions;
  }

  void setPartitions(int partitions) {
    this.partitions = partitions;
  }

  String getName() {
    return name;
  }

  void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("StreamEdge ");
    builder.append(getSystemStream().toString()).append(": (");
    List<String> sourceIds = sourceNodes.stream().map(node -> node.getId()).collect(Collectors.toList());
    String sources = Joiner.on(',').join(sourceIds);
    builder.append(sources).append(") -> (");
    List<String> targetIds = targetNodes.stream().map(node -> node.getId()).collect(Collectors.toList());
    String targets = Joiner.on(',').join(targetIds);
    builder.append(targets).append(")");
    return builder.toString();
  }
}
