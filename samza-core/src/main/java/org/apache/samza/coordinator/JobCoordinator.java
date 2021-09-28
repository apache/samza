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
 * A JobCoordinator is a pluggable module in each process that coordinates with workers to do processing.
 *
 * The process running a {@link JobCoordinator} can register a {@link JobCoordinatorListener} in order to get notified
 * about JobModel changes and coordinator state change.
 *
 * <pre>
 *   {@code
 *  *******************  start()                            ********************
 *  *                 *----------------------------------->>*                  *
 *  *                 *         onNewJobModel    ************                  *
 *  *                 *<<------------------------* Job      *                  *
 *  *                 *     onJobModelExpired    * Co-      *                  *
 *  * StreamProcessor *<<------------------------* ordinator*                  *
 *  *        or       *     onCoordinatorStop    * Listener *  JobCoordinator  *
 *  * other JC process*<<------------------------*          *                  *
 *  *                 *  onCoordinatorFailure    *          *                  *
 *  *                 *<<------------------------************                  *
 *  *                 *  stop()                             *                  *
 *  *                 *----------------------------------->>*                  *
 *  *******************                                     ********************
 *  }
 *  </pre>
 */
@InterfaceStability.Evolving
public interface JobCoordinator {
  /**
   * Starts the JobCoordinator, which generally consists of participating in LeaderElection and listening for JobModel
   * changes.
   */
  void start();

  /**
   * Stops the JobCoordinator and notifies the registered {@link JobCoordinatorListener}, if any
   */
  void stop();

  /**
   * Returns the identifier assigned to the processor that is local to the instance of StreamProcessor.
   *
   * The semantics and format of the identifier returned should adhere to the specification defined in
   * {@link org.apache.samza.runtime.ProcessorIdGenerator}
   *
   * @return String representing a unique logical processor ID
   */
  @Deprecated
  String getProcessorId();

  /**
   * Registers a {@link JobCoordinatorListener} to receive notification on coordinator state changes and job model changes
   *
   * @param listener An instance of {@link JobCoordinatorListener}
   */
  void setListener(JobCoordinatorListener listener);

  /**
   * Returns the current JobModel
   * The implementation of the JobCoordinator in the leader needs to know how to read the config and generate JobModel
   * In case of a non-leader, the JobCoordinator should simply fetch the jobmodel
   * @return instance of JobModel that describes the partition distribution among the processors (and hence, tasks)
   */
  JobModel getJobModel();
}
