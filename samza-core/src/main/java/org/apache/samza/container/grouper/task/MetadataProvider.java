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
import java.util.List;
import java.util.Map;

/**
 * Provides the historical metadata of the samza job.
 */
@InterfaceStability.Evolving
public interface MetadataProvider {

  /**
   * Gets the current processor locality of the job.
   * @return the processorId to the {@link LocationId} assignment.
   */
  Map<String, LocationId> getProcessorLocality();

  /**
   * Gets the current task locality of the job.
   * @return the current {@link TaskName} to {@link LocationId} assignment.
   */
  Map<TaskName, LocationId> getTaskLocality();

  /**
   * Gets the previous {@link TaskName} to {@link SystemStreamPartition} assignment of the job.
   * @return the previous {@link TaskName} to {@link SystemStreamPartition} assignment.
   */
  Map<TaskName, List<SystemStreamPartition>> getPreviousTaskToSSPAssignment();


  /**
   * Gets the previous {@link TaskName} to processorId assignments of the job.
   * @return the previous task to processorId assignment.
   */
  Map<TaskName, String> getPreviousTaskToProcessorAssignment();
}
