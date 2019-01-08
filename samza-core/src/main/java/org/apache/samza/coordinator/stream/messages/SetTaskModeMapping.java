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
 * SetTaskModeMapping is a {@link CoordinatorStreamMessage} used internally
 * by the Samza framework to persist the task-to-taskmode mappings.
 *
 * Structure of the message looks like:
 *
 * <pre>
 * key =&gt; [1, "set-task-mode-assignment", $TaskName]
 *
 * message =&gt; {
 *     "host": "192.168.0.1",
 *     "source": "SamzaTaskAssignmentManager",
 *     "username":"app",
 *     "timestamp": 1456177487325,
 *     "values": {
 *         "taskMode": "active"
 *     }
 * }
 * </pre>
 * */
public class SetTaskModeMapping extends CoordinatorStreamMessage {
  public static final String TYPE = "set-task-mode-assignment";
  public static final String TASKMODE_KEY = "taskMode";

  /**
   * SetTaskModeMapping is used to set the task to taskMode mapping information.
   * @param message which holds the mapped information.
   */
  public SetTaskModeMapping(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * SetTaskModeMapping is used to set the task to taskMode mapping information.
   * @param source              the source of the message
   * @param taskName                 the taskName which is used to persist the message
   * @param taskMode            the taskMode of the task
   */
  public SetTaskModeMapping(String source, String taskName, String taskMode) {
    super(source);
    setType(TYPE);
    setKey(taskName);
    putMessageValue(TASKMODE_KEY, taskMode.toString());
  }

  public String getTaskMode() {
    return getMessageValue(TASKMODE_KEY);
  }
}
