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

import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;

public abstract class CommandBuilder {
  protected Set<Partition> partitions;
  protected int totalPartitions;
  protected String name;
  protected Config config;

  public CommandBuilder setPartitions(Set<Partition> partitions) {
    this.partitions = partitions;
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

  public CommandBuilder setTotalPartitions(int totalPartitions) {
    this.totalPartitions = totalPartitions;
    return this;
  }

  public abstract String buildCommand();

  public abstract Map<String, String> buildEnvironment();
}
