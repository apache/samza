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
package org.apache.samza.coordinator.lifecycle;

import org.apache.samza.config.Config;


/**
 * Contains objects that are needed to build a {@link JobRestartSignal}.
 * Having this class allows {@link JobRestartSignalFactory#build} to remain unchanged if additional components
 * are needed in the future. Update this class if additional components are needed building {@link JobRestartSignal}.
 */
public class JobRestartSignalFactoryContext {
  private final Config config;

  public JobRestartSignalFactoryContext(Config config) {
    this.config = config;
  }

  /**
   * {@link Config} used to build a {@link JobRestartSignal}.
   */
  public Config getConfig() {
    return config;
  }
}
