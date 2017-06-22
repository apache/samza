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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.message.ControlMessage;
import org.apache.samza.message.IntermediateMessageType;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;


/**
 * This class delegates a control message to the corresponding manager.
 */
public class ControlMessageManager {

  interface ControlManager {
    IncomingMessageEnvelope update(IncomingMessageEnvelope envelope);
  }

  private final Map<IntermediateMessageType, ControlManager> managers;

  public ControlMessageManager(String taskName,
      JobModel jobModel,
      Set<SystemStreamPartition> ssps,
      Map<String, SystemAdmin> sysAdmins,
      MessageCollector collector) {
    Map<IntermediateMessageType, ControlManager> managerMap = new HashMap<>();
    managerMap.put(IntermediateMessageType.END_OF_STREAM_MESSAGE,
        new EndOfStreamManager(taskName, buildStreamToTasks(jobModel), ssps, sysAdmins, collector));
    managerMap.put(IntermediateMessageType.WATERMARK_MESSAGE,
        new WatermarkManager(taskName, buildStreamToTasks(jobModel), ssps, sysAdmins, collector));
    this.managers = Collections.unmodifiableMap(managerMap);
  }

  /**
   * Update the manager with a new control message. The manager of the control message type may
   * update its state, perform aggregations and then return the result envelope to the tasks.
   * @param controlMessage
   * @return
   */
  public IncomingMessageEnvelope update(IncomingMessageEnvelope controlMessage) {
    IntermediateMessageType type = IntermediateMessageType.of(controlMessage.getMessage());
    return managers.get(type).update(controlMessage);
  }

  /**
   * Send a control message to every partition of the {@link SystemStream}
   * @param message control message
   * @param systemStream the stream to sent
   * @param sysAdmins system admins
   * @param collector collector to send the message
   */
  static void sendControlMessage(ControlMessage message,
      SystemStream systemStream,
      Map<String, SystemAdmin> sysAdmins,
      MessageCollector collector) {
    String stream = systemStream.getStream();
    SystemStreamMetadata metadata = sysAdmins.get(systemStream.getSystem())
        .getSystemStreamMetadata(Collections.singleton(stream))
        .get(stream);
    int partitionCount = metadata.getSystemStreamPartitionMetadata().size();
    for (int i = 0; i < partitionCount; i++) {
      OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, i, "", message);
      collector.send(envelopeOut);
    }
  }

  /**
   * Build a map from a stream to its consumer tasks
   * @param jobModel job model which contains ssp -> task assignment
   * @return the map of input stream to tasks
   */
  static Multimap<SystemStream, String> buildStreamToTasks(JobModel jobModel) {
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
}
