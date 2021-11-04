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


public final class CoordinationConstants {
  private CoordinationConstants() {}

  public static final String RUNID_STORE_KEY = "runId";
  public static final String APPLICATION_RUNNER_PATH_SUFFIX = "ApplicationRunnerData";
  public static final String RUNID_LOCK_ID = "runId";
  public static final int LOCK_TIMEOUT_MS = 300000;

  // Yarn coordination constants for heartbeat
  public static final String YARN_CONTAINER_HEARTBEAT_SERVELET = "containerHeartbeat";
  public static final String YARN_EXECUTION_ENVIRONMENT_CONTAINER_ID = "executionContainerId";
  public static final String YARN_COORDINATOR_URL = "yarn.am.tracking.url";
  private static final String YARN_CONTAINER_HEARTBEAT_SERVLET_FORMAT = "%s" + YARN_CONTAINER_HEARTBEAT_SERVELET;
  private static final String YARN_CONTAINER_EXECUTION_ID_PARAM_FORMAT = YARN_EXECUTION_ENVIRONMENT_CONTAINER_ID + "=" + "%s";
  public static final String YARN_CONTAINER_HEARTBEAT_ENDPOINT_FORMAT = YARN_CONTAINER_HEARTBEAT_SERVLET_FORMAT + "?" +
      YARN_CONTAINER_EXECUTION_ID_PARAM_FORMAT;

  /**
   * Container name to use for job coordinator in components like metrics and diagnostics.
   */
  public static final String JOB_COORDINATOR_CONTAINER_NAME = "JobCoordinator";
}
