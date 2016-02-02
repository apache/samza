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

/**
 * The {@link SetChangelogMapping} message is used to store the changelog partition information for a particular task.
 * The structure looks like:
 * {
 * Key: TaskName
 * Type: set-changelog
 * Source: ContainerID
 * MessageMap:
 *  {
 *     "Partition" : partitionNumber (They key is just a dummy key here, the value contains the actual partition)
 *  }
 * }
 */
public class SetChangelogMapping extends CoordinatorStreamMessage {
  public static final String TYPE = "set-changelog";

  private static final String CHANGELOG_VALUE_KEY = "Partition";

  public SetChangelogMapping(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * The change log mapping message is used to store changelog partition information for a given task name.
   * @param source Source writing the change log mapping
   * @param taskName The task name to be used in the mapping
   * @param changelogPartitionNumber The partition to which the task's changelog is mapped to
   */
  public SetChangelogMapping(String source, String taskName, int changelogPartitionNumber) {
    super(source);
    setType(TYPE);
    setKey(taskName);
    putMessageValue(CHANGELOG_VALUE_KEY, String.valueOf(changelogPartitionNumber));
  }

  public String getTaskName() {
    return getKey();
  }

  public int getPartition() {
    return Integer.parseInt(getMessageValue(CHANGELOG_VALUE_KEY));
  }
}
