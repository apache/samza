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
 * SetTaskPartitionMapping is a {@link CoordinatorStreamMessage} used in samza
 * to persist the task to partition assignments of a samza job.
 *
 * Structure of the message:
 *
 * <pre>
 * key =&gt; [1, "set-task-partition-assignment", "{"partition": "1", "system": "test-system", "stream": "test-stream"}"]
 *
 * message =&gt; {
 *     "host" : "192.168.0.1",
 *     "source" : "TaskPartitionAssignmentManager",
 *     "username" :"app",
 *     "timestamp" : 1456177487325,
 *     "values" : {
 *         "taskNames" : ["task-1", "task2", "task3"]
 *     }
 * }
 * </pre>
 */
public class SetTaskPartitionMapping extends CoordinatorStreamMessage {

  private static final String TASK_NAME_KEY = "taskNames";

  public static final String TYPE = "set-task-partition-assignment";

  /**
   * SetTaskPartitionMapping is the data format for persisting the partition to task assignments
   * of a samza job to the coordinator stream.
   * @param message which holds the partition to task information.
   */
  public SetTaskPartitionMapping(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * SetTaskPartitionMapping is the data format for persisting the partition to task assignments
   * of a samza job to the coordinator stream.
   * @param source      the source of the message.
   * @param partition   the system stream partition serialized as a string.
   * @param taskName    the name of the task mapped to the system stream partition.
   */
  public SetTaskPartitionMapping(String source, String partition, String taskName) {
    super(source);
    setType(TYPE);
    setKey(partition);
    putMessageValue(TASK_NAME_KEY, taskName);
  }

  public String getTaskName() {
    return getMessageValue(TASK_NAME_KEY);
  }
}
