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
 * SetTaskContainerMapping is a {@link CoordinatorStreamMessage} used internally
 * by the Samza framework to persist the task-to-container mappings.
 *
 * Structure of the message looks like:
 *
 * <pre>
 * key =&gt; [1, "set-task-container-assignment", $TaskName]
 *
 * message =&gt; {
 *     "host": "192.168.0.1",
 *     "source": "SamzaTaskAssignmentManager",
 *     "username":"app",
 *     "timestamp": 1456177487325,
 *     "values": {
 *         "containerId": "139"
 *     }
 * }
 * </pre>
 * */
public class SetTaskContainerMapping extends CoordinatorStreamMessage {
  public static final String TYPE = "set-task-container-assignment";
  public static final String CONTAINER_KEY = "containerId";


  /**
   * SetContainerToHostMapping is used to set the container to host mapping information.
   * @param message which holds the container to host information.
   */
  public SetTaskContainerMapping(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * SteContainerToHostMapping is used to set the container to host mapping information.
   * @param source              the source of the message
   * @param taskName                 the taskName which is used to persist the message
   * @param containerId            the hostname of the container
   */
  public SetTaskContainerMapping(String source, String taskName, String containerId) {
    super(source);
    setType(TYPE);
    setKey(taskName);
    putMessageValue(CONTAINER_KEY, containerId);
  }

  public String getTaskAssignment() {
    return getMessageValue(CONTAINER_KEY);
  }


}
