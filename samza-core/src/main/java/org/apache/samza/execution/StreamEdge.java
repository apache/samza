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
import java.util.List;
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

  private String name = "";
  private int partitions = PARTITIONS_UNKNOWN;

  StreamEdge(StreamSpec streamSpec) {
    this.streamSpec = streamSpec;
    this.name = Util.getNameFromSystemStream(getSystemStream());
  }

  void addSourceNode(JobNode sourceNode) {
    sourceNodes.add(sourceNode);
  }

  void addTargetNode(JobNode targetNode) {
    targetNodes.add(targetNode);
  }

  public StreamSpec getStreamSpec() {
    if (partitions == PARTITIONS_UNKNOWN) {
      return streamSpec;
    } else {
      return streamSpec.copyWithPartitionCount(partitions);
    }
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
}
