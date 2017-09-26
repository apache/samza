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

package org.apache.samza.test.processor;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


public class IdentityStreamTask implements StreamTask , InitableTask  {
  private int processedMessageCount = 0;
  private int expectedMessageCount;
  private String outputTopic;
  private String outputSystem;

  @Override
  public void init(Config config, TaskContext taskContext) throws Exception {
    this.expectedMessageCount = config.getInt("app.messageCount");
    this.outputTopic = config.get("app.outputTopic", "output");
    this.outputSystem = config.get("app.outputSystem", "test-system");
  }

  @Override
  public void process(
      IncomingMessageEnvelope incomingMessageEnvelope,
      MessageCollector messageCollector,
      TaskCoordinator taskCoordinator) throws Exception {
    messageCollector.send(
        new OutgoingMessageEnvelope(
            new SystemStream(outputSystem, outputTopic),
            incomingMessageEnvelope.getMessage()));
    processedMessageCount++;
    if (processedMessageCount == expectedMessageCount) {
      taskCoordinator.shutdown(TaskCoordinator.RequestScope.ALL_TASKS_IN_CONTAINER);
    }
  }
}