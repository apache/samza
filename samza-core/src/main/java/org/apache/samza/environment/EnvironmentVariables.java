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
package org.apache.samza.environment;

/**
 * Contains environment variables that are used by Samza components. This provides a common place to put these variables
 * and provide documentation for them.
 */
public class EnvironmentVariables {
  /**
   * The properties of the epoch identifier are as follows
   * 1. Unique across applications in the cluster
   * 2. Remains unchanged within a single deployment lifecycle
   * 3. Remains unchanged across application attempt within a single deployment lifecycle
   * 4. Changes across deployment lifecycle
   *
   * If using a non-YARN environment, then this needs to be filled in so that the job coordinator can properly manage
   * changes (e.g. job model changes) within and across deployments.
   * See JobCoordinatorMetadataManager.fetchEpochIdForJobCoordinator for more details about the YARN alternative to this
   * environment variable.
   */
  public static final String SAMZA_EPOCH_ID = "SAMZA_EPOCH_ID";
}
