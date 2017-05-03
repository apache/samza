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
package org.apache.samza.coordinator;

import org.apache.samza.job.model.JobModel;

/**
 * Listener interface that can be registered with a {@link org.apache.samza.coordinator.JobCoordinator} instance in order
 * to receive notifications.
 */
public interface JobCoordinatorListener {
  /**
   * Method invoked by a {@link org.apache.samza.coordinator.JobCoordinator} in the following scenarios:
   * <ul>
   *  <li>the existing {@link JobModel} is no longer valid due to either re-balancing </li>
   *  <li>JobCoordinator is shutting down</li>
   * </ul>
   */
  void onJobModelExpired();
  
  /**
   * Method invoked by a {@link org.apache.samza.coordinator.JobCoordinator} when there is new {@link JobModel}
   * available for use by the processor.
   *
   * @param processorId String, representing the identifier of {@link org.apache.samza.processor.StreamProcessor}
   * @param jobModel Current {@link JobModel} containing a {@link org.apache.samza.job.model.ContainerModel} for the
   *                 given processorId
   */
  // TODO: Can change interface to ContainerModel if maxChangelogStreamPartitions can be made a part of ContainerModel
  void onNewJobModel(String processorId, JobModel jobModel);

  /**
   * Method invoked by a {@link org.apache.samza.coordinator.JobCoordinator} when it is shutting without any errors
   */
  void onCoordinatorStop();

  /**
   *
   * Method invoked by a {@link org.apache.samza.coordinator.JobCoordinator} when it is shutting down with error.
   * <b>Note</b>: This should be the last call after completely shutting down the JobCoordinator.
   *
   * @param t Throwable that was the cause of the JobCoordinator failure
   */
  void onCoordinatorFailure(Throwable t);
}
