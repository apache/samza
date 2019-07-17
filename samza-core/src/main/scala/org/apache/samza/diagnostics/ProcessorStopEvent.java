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
package org.apache.samza.diagnostics;

import java.util.Objects;


/**
 * Encapsulates information (emitted to diagnostics stream) about the stopping of a processor.
 * Information emitted includes, processorId, resourceId, exit status and host.
 */
public class ProcessorStopEvent {
  public final String processorId;
  public final String resourceId;
  public final String host;
  public final int exitStatus;

  // Default constructor, required for deserialization with jackson
  private ProcessorStopEvent() {
    this("", "", "", -1);
  }

  public ProcessorStopEvent(String processorId, String resourceId, String host, int exitStatus) {
    this.processorId = processorId;
    this.resourceId = resourceId;
    this.host = host;
    this.exitStatus = exitStatus;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProcessorStopEvent that = (ProcessorStopEvent) o;
    return exitStatus == that.exitStatus && Objects.equals(processorId, that.processorId) && Objects.equals(
        resourceId, that.resourceId) && Objects.equals(host, that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(processorId, resourceId, host, exitStatus);
  }
}
