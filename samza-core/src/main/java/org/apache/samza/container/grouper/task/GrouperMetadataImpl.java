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

import org.apache.samza.container.TaskName;
import org.apache.samza.runtime.LocationId;
import org.apache.samza.system.SystemStreamPartition;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link GrouperMetadata} that holds the necessary historical metadata of
 * the samza job. This is used by the {@link TaskNameGrouper} to generate optimal task assignments.
 */
public class GrouperMetadataImpl implements GrouperMetadata {

  // Map of processorId to LocationId.
  private final Map<String, LocationId> processorLocality;

  // Map of TaskName to LocationId.
  private final Map<TaskName, LocationId> taskLocality;

  // Map of TaskName to a list of the input SystemStreamPartition's assigned to it.
  private final Map<TaskName, List<SystemStreamPartition>> previousTaskToSSPAssignment;

  // Map of TaskName to ProcessorId.
  private final Map<TaskName, String> previousTaskToProcessorAssignment;

  public GrouperMetadataImpl(Map<String, LocationId> processorLocality, Map<TaskName, LocationId> taskLocality, Map<TaskName, List<SystemStreamPartition>> previousTaskToSSPAssignments, Map<TaskName, String> previousTaskToProcessorAssignment) {
    this.processorLocality = Collections.unmodifiableMap(processorLocality);
    this.taskLocality = Collections.unmodifiableMap(taskLocality);
    this.previousTaskToSSPAssignment = Collections.unmodifiableMap(previousTaskToSSPAssignments);
    this.previousTaskToProcessorAssignment = Collections.unmodifiableMap(previousTaskToProcessorAssignment);
  }

  @Override
  public Map<String, LocationId> getProcessorLocality() {
    return processorLocality;
  }

  @Override
  public Map<TaskName, LocationId> getTaskLocality() {
    return taskLocality;
  }

  @Override
  public Map<TaskName, List<SystemStreamPartition>> getPreviousTaskToSSPAssignment() {
    return previousTaskToSSPAssignment;
  }

  @Override
  public Map<TaskName, String> getPreviousTaskToProcessorAssignment() {
    return this.previousTaskToProcessorAssignment;
  }
}
