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

package org.apache.samza.test.integration;

import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskCoordinator.RequestScope;
import org.apache.samza.util.Util;

/**
 * A simple test job that reads strings, converts them to integers, multiplies
 * by -1, and outputs to "samza-test-topic-output" stream.
 */
public class NegateNumberTask implements StreamTask, InitableTask {
  /**
   * How many messages the all tasks in a single container have processed.
   */
  private static int messagesProcessed = 0;

  /**
   * How many messages to process before shutting down.
   */
  private int maxMessages;

  /**
   * The SystemStream to send negated numbers to.
   */
  private SystemStream outputSystemStream;

  @Override
  public void init(Config config, TaskContext context) throws Exception {
    maxMessages = config.getInt("task.max.messages", 50);
    String outputSystemStreamString = config.get("task.outputs", null);
    if (outputSystemStreamString == null) {
      throw new ConfigException("Missing required configuration: task.outputs");
    }
    outputSystemStream = Util.getSystemStreamFromNames(outputSystemStreamString);
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    messagesProcessed += 1;
    String input = (String) envelope.getMessage();
    Integer number = Integer.valueOf(input);
    Integer output = number.intValue() * -1;
    collector.send(new OutgoingMessageEnvelope(outputSystemStream, output.toString()));
    if (messagesProcessed >= maxMessages) {
      coordinator.shutdown(RequestScope.ALL_TASKS_IN_CONTAINER);
    }
  }
}
