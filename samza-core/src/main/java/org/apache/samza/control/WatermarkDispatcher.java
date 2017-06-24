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

package org.apache.samza.control;

import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.operators.util.IOGraphUtil;
import org.apache.samza.system.SystemStream;


/**
 * The dispatcher class propagates the watermark messages to downstream.
 * It calculates the producer task count to each stream, and send the watermark message with
 * (timestamp, current task, task count) to all partitions of the intermediate streams.
 */
public class WatermarkDispatcher {
  private final Collection<IOGraphUtil.IONode> ioGraph;
  private Map<SystemStream, Integer> outputTaskCount = null;

  /**
   * Create the dispatcher for end-of-stream messages based on the IOGraph built on StreamGraph
   * @param ioGraph collections of {@link org.apache.samza.operators.util.IOGraphUtil.IONode}
   */
  public WatermarkDispatcher(Collection<IOGraphUtil.IONode> ioGraph) {
    this.ioGraph = ioGraph;
  }

  public void propagate(Watermark watermark, SystemStream systemStream) {
    final WatermarkManager manager = ((WatermarkManager.WatermarkImpl) watermark).getManager();
    if (outputTaskCount == null) {
      outputTaskCount = buildOutputToTaskCountMap(manager.getStreamToTasks());
    }
    manager.sendWatermark(watermark.getTimestamp(), systemStream, outputTaskCount.get(systemStream));
  }

  private Map<SystemStream, Integer> buildOutputToTaskCountMap(Multimap<SystemStream, String> streamToTasks) {
    Map<SystemStream, Integer> outputTaskCount = new HashMap<>();
    ioGraph.forEach(node -> {
        int count = (int) node.getInputs().stream().flatMap(spec -> streamToTasks.get(spec.toSystemStream()).stream())
            .collect(Collectors.toSet()).size();
        outputTaskCount.put(node.getOutput().toSystemStream(), count);
      });
    return outputTaskCount;
  }
}
