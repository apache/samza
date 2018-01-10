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
package org.apache.samza.application;

import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.runtime.ApplicationRuntimeResult;


/**
 * Interface method for all runtime instance of applications
 */
public interface ApplicationRunnable {
  /**
   * Deploy and run the Samza jobs to execute this application.
   * The behavior of the method is dependant on the {@link org.apache.samza.runtime.ApplicationRunner} configured for this application
   */
  ApplicationRuntimeResult run();

  /**
   * Kill the Samza jobs represented by this application
   * The behavior of the method is dependant on the {@link org.apache.samza.runtime.ApplicationRunner} configured for this application
   */
  ApplicationRuntimeResult kill();

  /**
   * Get the collective status of the Samza jobs represented by this application.
   * Returns {@link ApplicationStatus} running if all jobs are running.
   * The behavior of the method is dependant on the {@link org.apache.samza.runtime.ApplicationRunner} configured for this application
   *
   * @return the status of the application
   */
  ApplicationStatus status();

}
