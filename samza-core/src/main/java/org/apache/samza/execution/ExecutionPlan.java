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

package org.apache.samza.execution;

import java.util.List;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.system.StreamSpec;


/**
 * This interface represents Samza {@link org.apache.samza.application.StreamApplication}
 * plans for physical execution.
 */
@InterfaceStability.Unstable
public interface ExecutionPlan {

  /**
   * Returns the configs for single stage job, in topological sort order.
   * @return list of job configs
   */
  List<JobConfig> getJobConfigs();

  /**
   * Returns the config for this application
   * @return {@link ApplicationConfig}
   */
  ApplicationConfig getApplicationConfig();

  /**
   * Returns the intermediate streams that need to be created.
   * @return intermediate {@link StreamSpec}s
   */
  List<StreamSpec> getIntermediateStreams();

  /**
   * Returns the JSON representation of the plan.
   * @return JSON string
   * @throws Exception exception during JSON serialization, including {@link java.io.IOException}
   *                   and {@link org.codehaus.jackson.JsonGenerationException}
   */
  String getPlanAsJson() throws Exception;
}
