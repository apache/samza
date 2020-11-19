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
 * SetContainerIdExecutionContainerIdMapping is used internally by the Samza framework to
 * persist the container processorId-to-executionEnvContainerId mappings.
 *
 * Structure of the message looks like:
 * {
 *     Key: $ContainerId
 *     Type: set-container-id-execution-id-assignment
 *     Source: "SamzaContainer-$ContainerId"
 *     MessageMap:
 *     {
 *         execution-env-container-id: execution environment container id
 *     }
 * }
 * */
public class SetExecutionContainerIdMapping extends CoordinatorStreamMessage {
  public static final String TYPE = "set-container-id-execution-id-assignment";
  public static final String EXEC_ENV_ID_KEY = "execution-env-container-id";

  /**
   * SteContainerToHostMapping is used to set the container to host mapping information.
   * @param message which holds the container to host information.
   */
  public SetExecutionContainerIdMapping(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * SteContainerToHostMapping is used to set the container to host mapping information.
   * @param source the source of the message
   * @param key the key which is used to persist the message
   * @param executionEnvContainerId the execution environment container id
   */
  public SetExecutionContainerIdMapping(String source, String key, String executionEnvContainerId) {
    super(source);
    setType(TYPE);
    setKey(key);
    putMessageValue(EXEC_ENV_ID_KEY, executionEnvContainerId);
  }

  public String getExecutionEnvironmentContainerId() {
    return getMessageValue(EXEC_ENV_ID_KEY);
  }
}
