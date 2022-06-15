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
package org.apache.samza.drain;

import com.google.common.base.Objects;
import java.util.UUID;

/**
 * DrainNotification is a custom message is used by an external controller to trigger Drain.
 * The message is written in the metadata store using {@link DrainUtils}.
 * */
public class DrainNotification {
  /**
   * Unique identifier of a drain notification.
   */
  private final UUID uuid;
  /**
   * Unique identifier for a deployment so drain notifications messages can be invalidated across a job restarts.
   */
  private final String runId;

  public DrainNotification(UUID uuid, String runId) {
    this.uuid = uuid;
    this.runId = runId;
  }

  public UUID getUuid() {
    return this.uuid;
  }

  public String getRunId() {
    return runId;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("DrainMessage{");
    sb.append(" UUID: ").append(uuid);
    sb.append(", runId: '").append(runId).append('\'');
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DrainNotification that = (DrainNotification) o;
    return Objects.equal(uuid, that.uuid)
        && Objects.equal(runId, that.runId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(uuid, runId);
  }
}
