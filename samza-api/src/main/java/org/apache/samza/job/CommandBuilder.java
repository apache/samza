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

package org.apache.samza.job;

import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Map;
import java.util.Set;

/**
 * CommandBuilders are used to customize the command necessary to launch a Samza Job for a particular framework,
 * such as YARN or the LocalJobRunner.
 */
public abstract class CommandBuilder {
  protected Set<SystemStreamPartition> systemStreamPartitions;
  protected String name;
  protected Config config;

  public CommandBuilder setStreamPartitions(Set<SystemStreamPartition> ssp) {
    this.systemStreamPartitions = ssp;
    return this;
  }
  
  /**
   * @param name
   *          associated with a specific instantiation of a TaskRunner.
   * @return self to support a builder style of use.
   */
  public CommandBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public CommandBuilder setConfig(Config config) {
    this.config = config;
    return this;
  }

  public abstract String buildCommand();

  public abstract Map<String, String> buildEnvironment();
}
