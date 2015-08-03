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

package org.apache.samza.coordinator.stream.messages;

import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Util;

import java.util.HashMap;
import java.util.Map;

/**
 * The SetCheckpoint is used to store the checkpoint messages for a particular task.
 * The structure looks like:
 * {
 * Key: TaskName
 * Type: set-checkpoint
 * Source: ContainerID
 * MessageMap:
 *  {
 *     SSP1 : offset,
 *     SSP2 : offset
 *  }
 * }
 */
public class SetCheckpoint extends CoordinatorStreamMessage {
  public static final String TYPE = "set-checkpoint";

  public SetCheckpoint(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * The SetCheckpoint is used to store checkpoint message for a given task.
   *
   * @param source The source writing the checkpoint
   * @param key The key for the checkpoint message (Typically task name)
   * @param checkpoint Checkpoint message to be written to the stream
   */
  public SetCheckpoint(String source, String key, Checkpoint checkpoint) {
    super(source);
    setType(TYPE);
    setKey(key);
    Map<SystemStreamPartition, String> offsets = checkpoint.getOffsets();
    for (Map.Entry<SystemStreamPartition, String> systemStreamPartitionStringEntry : offsets.entrySet()) {
      putMessageValue(Util.sspToString(systemStreamPartitionStringEntry.getKey()), systemStreamPartitionStringEntry.getValue());
    }
  }

  public Checkpoint getCheckpoint() {
    Map<SystemStreamPartition, String> offsetMap = new HashMap<SystemStreamPartition, String>();
    for (Map.Entry<String, String> sspToOffsetEntry : getMessageValues().entrySet()) {
      offsetMap.put(Util.stringToSsp(sspToOffsetEntry.getKey()), sspToOffsetEntry.getValue());
    }
    return new Checkpoint(offsetMap);
  }
}
