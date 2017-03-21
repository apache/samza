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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.job.model.JobModel;

/**
 *  A JobCoordinator is a pluggable module in each process that provides the JobModel and the ID to the StreamProcessor.
 *  In some cases, ID assignment is completely config driven, while in other cases, ID assignment may require
 *  coordination with JobCoordinators of other StreamProcessors.
 *  */
@InterfaceStability.Evolving
public interface JobCoordinator {
  /**
   * Starts the JobCoordinator which involves one or more of the following:
   * * LeaderElector Module initialization, if any
   * * If leader, generate JobModel. Else, read JobModel
   */
  void start();

  /**
   * Cleanly shutting down the JobCoordinator involves:
   * * Shutting down the Container
   * * Shutting down the LeaderElection module (TBD: details depending on leader or not)
   */
  void stop();

  /**
   * Waits for a specified amount of time for the JobCoordinator to fully start-up, which means it should be ready to
   * process messages.
   * In a Standalone use-case, it may be sufficient to wait for the container to start-up.
   * In a ZK based Standalone use-case, it also includes registration with ZK, initialization of the
   * leader elector module, container start-up etc.
   *
   * @param timeoutMs Maximum time to wait, in milliseconds
   * @return {@code true}, if the JobCoordinator is started within the specified wait time and {@code false} if the
   * waiting time elapsed
   * @throws InterruptedException if the current thread is interrupted while waiting for the JobCoordinator to start-up
   */
  boolean awaitStart(long timeoutMs) throws InterruptedException;

  boolean awaitStop(long timeoutMs) throws InterruptedException;

  /**
   * Returns the logical ID assigned to the processor
   * It is up to the user to ensure that different instances of StreamProcessor within a job have unique processor ID.
   * @return integer representing the logical processor ID
   */
  int getProcessorId();

  /**
   * Returns the current JobModel
   * The implementation of the JobCoordinator in the leader needs to know how to read the config and generate JobModel
   * In case of a non-leader, the JobCoordinator should simply fetch the jobmodel
   * @return instance of JobModel that describes the partition distribution among the processors (and hence, tasks)
   */
  JobModel getJobModel();
}
