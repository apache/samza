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
import org.apache.samza.task.ClosableTask;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.concurrent.CountDownLatch;

public class IdentityStreamTask implements StreamTask , InitableTask , ClosableTask  {
  // static field since there's no other way to share state b/w a task instance and
  // stream processor when constructed from "task.class".
  static CountDownLatch endLatch = new CountDownLatch(1);
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
      endLatch.countDown();
    }
  }

  @Override
  public void close() throws Exception {
    // need to create a new latch after each test since it's a static field.
    // tests are assumed to run sequentially.
    endLatch = new CountDownLatch(1);
  }
}