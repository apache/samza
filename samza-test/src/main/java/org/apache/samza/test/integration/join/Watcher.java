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

package org.apache.samza.test.integration.join;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Watcher implements StreamTask, WindowableTask, InitableTask {
  
  private static Logger logger = LoggerFactory.getLogger(Watcher.class);

  private boolean inError = false;
  private long lastEpochChange = System.currentTimeMillis();
  private long maxTimeBetweenEpochsMs;
  private int currentEpoch = 0;
  
  @Override
  public void init(Config config, TaskContext context) {
    this.maxTimeBetweenEpochsMs = config.getLong("max.time.between.epochs.ms");
  }
  
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    int epoch = Integer.parseInt((String) envelope.getMessage());
    if (epoch > currentEpoch) {
      logger.info("Epoch changed to " + epoch + " from " + currentEpoch);
      this.currentEpoch = epoch;
      this.lastEpochChange = System.currentTimeMillis();
      this.inError = false;
    }
  }
  
  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    boolean isLagging = System.currentTimeMillis() - lastEpochChange > maxTimeBetweenEpochsMs;
    if (!inError && isLagging) {
      this.inError = true;
      logger.info("Error state detected, alerting...");
      logger.error("Job failed to make progress!" + String.format("No epoch change for %d minutes.", this.maxTimeBetweenEpochsMs / (60 * 1000)));
    }
  }
  
}
