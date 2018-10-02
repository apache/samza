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
package org.apache.samza.context;

import org.apache.samza.config.Config;


/**
 * Contains information at job granularity, provided by the Samza framework, to be used to instantiate an application at
 * runtime.
 */
public interface JobContext {
  /**
   * Returns the final configuration for this job.
   * @return configuration for this job
   */
  Config getConfig();

  /**
   * Returns the name of the job.
   * @return name of the job
   */
  String getJobName();

  /**
   * Returns the instance id for this instance of this job.
   * @return instance id for the job
   */
  String getJobId();
}
