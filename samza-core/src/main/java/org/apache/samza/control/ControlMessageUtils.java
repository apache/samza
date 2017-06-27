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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.message.ControlMessage;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.task.MessageCollector;


/**
 * This class privates static utils for handling control messages
 */
public class ControlMessageUtils {

  /**
   * Send a control message to every partition of the {@link SystemStream}
   * @param message control message
   * @param systemStream the stream to sent
   * @param metadataCache stream metadata cache
   * @param collector collector to send the message
   */
  public static void sendControlMessage(ControlMessage message,
      SystemStream systemStream,
      StreamMetadataCache metadataCache,
      MessageCollector collector) {
    SystemStreamMetadata metadata = metadataCache.getSystemStreamMetadata(systemStream, true);
    int partitionCount = metadata.getSystemStreamPartitionMetadata().size();
    for (int i = 0; i < partitionCount; i++) {
      OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, i, "", message);
      collector.send(envelopeOut);
    }
  }

  /**
   * Build a map from a stream to its consumer tasks
   * @param jobModel job model which contains ssp-to-task assignment
   * @return the map of input stream to tasks
   */
  public static Multimap<SystemStream, String> buildInputToTasks(JobModel jobModel) {
    Multimap<SystemStream, String> streamToTasks = HashMultimap.create();
    if (jobModel != null) {
      jobModel.getContainers().values().forEach(containerModel -> {
          containerModel.getTasks().values().forEach(taskModel -> {
              taskModel.getSystemStreamPartitions().forEach(ssp -> {
                  streamToTasks.put(ssp.getSystemStream(), taskModel.getTaskName().toString());
                });
            });
        });
    }
    return streamToTasks;
  }

  /**
   * Calculate the mapping from an output stream to the number of upstream tasks that will produce to the output stream
   * @param inputToTasks input stream to its consumer tasks mapping
   * @param ioGraph topology of the stream inputs and outputs
   * @return mapping from output to upstream task count
   */
  public static Map<SystemStream, Integer> calculateUpstreamTaskCounts(Multimap<SystemStream, String> inputToTasks,
      IOGraph ioGraph) {
    Map<SystemStream, Integer> outputTaskCount = new HashMap<>();
    ioGraph.getNodes().forEach(node -> {
        int count = node.getInputs().stream().flatMap(spec -> inputToTasks.get(spec.toSystemStream()).stream())
            .collect(Collectors.toSet()).size();
        outputTaskCount.put(node.getOutput().toSystemStream(), count);
      });
    return outputTaskCount;
  }
}
