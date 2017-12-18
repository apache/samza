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

/**
 * The abstract class of all control messages, containing
 * the task that produces the control message, the total number of producer tasks,
 * and a version number.
 */
public abstract class ControlMessage {
  private final String taskName;
  private int version = 1;

  public ControlMessage(String taskName) {
    this.taskName = taskName;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public int getVersion() {
    return version;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = Integer.hashCode(version);
    result = prime * result + (taskName != null ? taskName.hashCode() : 0);
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

    final ControlMessage other = (ControlMessage) obj;
    if (version != other.version) {
      return false;
    }

    if (taskName != null
        ? !taskName.equals(other.getTaskName())
        : other.taskName != null) {
      return false;
    }

    return true;
  }
}