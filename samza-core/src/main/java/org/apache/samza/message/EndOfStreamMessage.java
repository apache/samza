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

package org.apache.samza.message;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;


public class EndOfStreamMessage extends ControlMessage {
  private final String taskName;
  private final int taskCount;

  @JsonCreator
  public EndOfStreamMessage(@JsonProperty("task-name") String taskName, @JsonProperty("task-count") int taskCount) {
    this.taskName = taskName;
    this.taskCount = taskCount;
  }

  @JsonProperty("task-name")
  public String getTaskName() {
    return taskName;
  }

  @JsonProperty("task-count")
  public int getTaskCount() {
    return taskCount;
  }
}
