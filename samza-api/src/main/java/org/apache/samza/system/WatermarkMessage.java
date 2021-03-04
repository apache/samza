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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *  The WatermarkMessage is a control message that is sent out to next stage
 *  with a watermark timestamp and the task that produces the watermark.
 */
public class WatermarkMessage extends ControlMessage {
  private final long timestamp;

  public WatermarkMessage(long watermark) {
    this(watermark, null);
  }

  @JsonCreator
  public WatermarkMessage(@JsonProperty("timestamp") long timestamp,
                          @JsonProperty("task-name") String taskName) {
    super(taskName);
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + Long.hashCode(timestamp);
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
    if (!super.equals(obj))
      return false;

    final WatermarkMessage other = (WatermarkMessage) obj;
    if (timestamp != other.timestamp) {
      return false;
    }
    return true;
  }
}
