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
package org.apache.samza.checkpoint;

import org.apache.samza.config.Config;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.util.Util;
import scala.Option;

/**
 * Checkpoint manager utility class.
 */
public class CheckpointManagerUtil {

  /**
   * Creates and initlizes the checkpoint manager from config.
   *
   * @param config Configuration with checkpoint manager properties
   * @param metricsRegistry Metrics registry for the checkpoint manager to use.
   * @return CheckpointManager.
   */
  public static CheckpointManager createAndInit(Config config, MetricsRegistry metricsRegistry) {
    // Initialize checkpoint streams during job coordination
    Option<String> checkpointManagerFactoryName = new TaskConfig(config).getCheckpointManagerFactory();
    if (checkpointManagerFactoryName.isDefined()) {
      CheckpointManager checkpointManager =
          Util.<CheckpointManagerFactory>getObj(checkpointManagerFactoryName.get()).getCheckpointManager(config, metricsRegistry);
      checkpointManager.init();
      return checkpointManager;
    }
    return null;
  }
}
