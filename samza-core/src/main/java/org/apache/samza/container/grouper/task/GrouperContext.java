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
package org.apache.samza.container.grouper.task;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.container.TaskName;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.system.SystemStreamPartition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A Wrapper class that holds the necessary historical metadata of the samza job which is used
 * by the {@link org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper}
 * and {@link TaskNameGrouper} to generate optimal task assignments.
 */
@InterfaceStability.Evolving
public class GrouperContext {
  private Map<String, LocationId> processorLocality;
  private Map<TaskName, LocationId> taskLocality;
  private Map<TaskName, List<SystemStreamPartition>> previousTaskToSSPAssignment;
  private Map<TaskName, String> previousTaskToContainerAssignment;

  public GrouperContext(Map<String, LocationId> processorLocality, Map<TaskName, LocationId> taskLocality, Map<TaskName, List<SystemStreamPartition>> previousTaskToSSPAssignments, Map<TaskName, String> previousTaskToContainerAssignment) {
    this.processorLocality = processorLocality;
    this.taskLocality = taskLocality;
    this.previousTaskToSSPAssignment = previousTaskToSSPAssignments;
    this.previousTaskToContainerAssignment = previousTaskToContainerAssignment;
  }

  public Map<String, LocationId> getProcessorLocality() {
    return Collections.unmodifiableMap(processorLocality);
  }

  public Map<TaskName, LocationId> getTaskLocality() {
    return Collections.unmodifiableMap(taskLocality);
  }

  public Map<TaskName, List<SystemStreamPartition>> getPreviousTaskToSSPAssignment() {
    return Collections.unmodifiableMap(previousTaskToSSPAssignment);
  }

  public List<String> getProcessorIds() {
    return new ArrayList<>(processorLocality.keySet());
  }

  public Map<TaskName, String> getPreviousTaskToContainerAssignment() {
    return Collections.unmodifiableMap(this.previousTaskToContainerAssignment);
  }
}
