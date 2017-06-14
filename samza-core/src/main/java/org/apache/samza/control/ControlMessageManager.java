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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.message.IntermediateMessageType;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
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
      int taskCount,
      Set<SystemStreamPartition> ssps,
      Map<String, SystemAdmin> sysAdmins,
      MessageCollector collector) {
    Map<IntermediateMessageType, ControlManager> managerMap = new HashMap<>();
    managerMap.put(IntermediateMessageType.END_OF_STREAM_MESSAGE, new EndOfStreamManager(taskName, taskCount, ssps, sysAdmins, collector));
    this.managers = Collections.unmodifiableMap(managerMap);
  }

  public IncomingMessageEnvelope update(IncomingMessageEnvelope controlMessage) {
    IntermediateMessageType type = IntermediateMessageType.of(controlMessage.getMessage());
    return managers.get(type).update(controlMessage);
  }
}
