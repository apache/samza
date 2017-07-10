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

public class SetSSPTaskMapping extends CoordinatorStreamMessage {
  public static final String TYPE = "set-ssp-task-assignment";
  public static final String TASK_NAME_KEY = "taskName";


  /**
   * SetSSPTaskMapping is used to set the ssp to task mapping information.
   * @param message which holds the ssp to task mapping information.
   */
  public SetSSPTaskMapping(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * SetSSPTaskMapping is used to set the ssp to task mapping information.
   * @param source              the source of the message
   * @param ssp                 the systemSteamPartition
   * @param taskName            the taskName
   */
  public SetSSPTaskMapping(String source, String ssp, String taskName) {
    super(source);
    setType(TYPE);
    setKey(ssp);
    putMessageValue(TASK_NAME_KEY, taskName);
  }

  public String getTaskAssignment() {
    return getMessageValue(TASK_NAME_KEY);
  }


}
