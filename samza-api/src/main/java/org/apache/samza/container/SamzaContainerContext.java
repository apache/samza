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

package org.apache.samza.container;

import java.util.Collection;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;

/**
 * A SamzaContainerContext maintains per-container information for the tasks it executes.
 */
public class SamzaContainerContext {
  public final String name;
  public final Config config;
  public final Collection<Partition> partitions;

  /**
   * An immutable context object that can passed to tasks to give them information
   * about the container in which they are executing.
   * @param name The name of the container (either a YARN AM or SamzaContainer).
   * @param config The job configuration.
   * @param partitions The set of input partitions assigned to this container.
   */
  public SamzaContainerContext(
      String name,
      Config config,
      Collection<Partition> partitions) {
    this.name = name;
    this.config = config;
    this.partitions = partitions;
  }
}
