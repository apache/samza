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
package org.apache.samza.lineage;

import org.apache.samza.config.Config;


/**
 * The LineageFactory class helps parse and generate job lineage information from Samza job config.
 */
public interface LineageFactory<T> {

  /**
   * Parse and generate job lineage data model from Samza job config, the data model includes job's inputs, outputs and
   * other metadata information.
   * The config should include full inputs and outputs information, those information are usually populated by job
   * planner and config rewriters.
   * It is the user's responsibility to make sure config contains all required information before call this function.
   *
   * @param config Samza job config
   * @return Samza job lineage data model
   */
  T getLineage(Config config);
}
