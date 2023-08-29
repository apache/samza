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

import org.apache.samza.config.Config;
import org.apache.samza.config.RunLoopConfig;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.util.HighResolutionClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import java.util.concurrent.ExecutorService;

/**
 * Factory class to create runloop for a Samza task, based on the type
 * of the task
 */
public class RunLoopFactory {
  private static final Logger log = LoggerFactory.getLogger(RunLoopFactory.class);

  public static Runnable createRunLoop(scala.collection.immutable.Map<TaskName, RunLoopTask> taskInstances,
      SystemConsumers consumerMultiplexer,
      ExecutorService threadPool,
      SamzaContainerMetrics containerMetrics,
      HighResolutionClock clock,
      Config config) {

    log.info("Run loop in asynchronous mode.");

    return new RunLoop(
      JavaConverters.mapAsJavaMapConverter(taskInstances).asJava(),
      threadPool,
      consumerMultiplexer,
      containerMetrics,
      clock,
      new RunLoopConfig(config));
  }
}
