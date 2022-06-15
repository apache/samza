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

package org.apache.samza.system;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The DrainMessage is a control message that is sent out to next stage
 * once the task has consumed to the end of a bounded stream.
 */
public class DrainMessage extends ControlMessage {
  /**
   * Id used to invalidate DrainMessages between runs. Ties to app.run.id from config.
   */
  private final String runId;

  public DrainMessage(String runId) {
    this(null, runId);
  }

  public DrainMessage(@JsonProperty("task-name") String taskName, @JsonProperty("run-id") String runId) {
    super(taskName);
    this.runId = runId;
  }

  public String getRunId() {
    return runId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    final int result = prime * super.hashCode() + (this.runId != null ? this.runId.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;

    final DrainMessage other = (DrainMessage) obj;
    if (!super.equals(other)) {
      return false;
    }
    if (!runId.equals(other.runId)) {
      return false;
    }
    return true;
  }
}
