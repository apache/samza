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
package org.apache.samza.container.grouper.task;

import org.apache.samza.config.Config;

/**
 * Factory for building a {@link TaskNameGrouper}.
 */
public interface TaskNameGrouperFactory {
  /**
   * Builds a {@link TaskNameGrouper}. The config can be used to read the necessary values which are needed int the
   * process of building the {@link TaskNameGrouper}
   *
   * @param config configuration to which values can be used to build a {@link TaskNameGrouper}
   * @return a {@link TaskNameGrouper} implementation
   */
  TaskNameGrouper build(Config config);
}
