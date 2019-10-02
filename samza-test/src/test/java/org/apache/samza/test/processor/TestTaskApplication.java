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

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.ClosableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.table.TestTableData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTaskApplication implements TaskApplication {

  private static final Logger LOG = LoggerFactory.getLogger(TestTaskApplication.class);

  private final String systemName;
  private final String inputTopic;
  private final String outputTopic;
  private final CountDownLatch shutdownLatch;
  private final CountDownLatch processedMessageLatch;
  private final Optional<TaskApplicationProcessCallback> processCallback;

  /**
   * A test TaskApplication to use in test harnesses.
   * @param systemName test input/output system
   * @param inputTopic topic to consume
   * @param outputTopic topic to output
   * @param processedMessageLatch latch that counts down per message processed
   * @param shutdownLatch latch that counts down once during shutdown
   */
  public TestTaskApplication(String systemName, String inputTopic, String outputTopic,
      CountDownLatch processedMessageLatch, CountDownLatch shutdownLatch) {
    this(systemName, inputTopic, outputTopic, processedMessageLatch, shutdownLatch, Optional.empty());
  }

  /**
   * A test TaskApplication to use in test harnesses.
   * @param systemName test input/output system
   * @param inputTopic topic to consume
   * @param outputTopic topic to output
   * @param processedMessageLatch latch that counts down per message processed
   * @param shutdownLatch latch that counts down once during shutdown
   * @param processCallback optional callback called per message processed.
   */
  public TestTaskApplication(String systemName, String inputTopic, String outputTopic,
      CountDownLatch processedMessageLatch, CountDownLatch shutdownLatch, Optional<TaskApplicationProcessCallback> processCallback) {
    this.systemName = systemName;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.processedMessageLatch = processedMessageLatch;
    this.shutdownLatch = shutdownLatch;
    this.processCallback = processCallback;
  }

  private class TestTaskImpl implements AsyncStreamTask, ClosableTask {

    @Override
    public void processAsync(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator, TaskCallback callback) {
      processedMessageLatch.countDown();
      // Implementation does not invokes callback.complete to block the RunLoop.process() after it exhausts the
      // `task.max.concurrency` defined per task. Call callback.complete() in the processCallback if needed.
      processCallback.ifPresent(pcb -> pcb.onMessage(envelope, callback));
    }

    @Override
    public void close() {
      LOG.info("Task instance is shutting down.");
      shutdownLatch.countDown();
    }
  }

  @Override
  public void describe(TaskApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor(systemName);
    KafkaInputDescriptor<TestTableData.Profile> inputDescriptor = ksd.getInputDescriptor(inputTopic, new NoOpSerde<>());
    KafkaOutputDescriptor<TestTableData.EnrichedPageView> outputDescriptor = ksd.getOutputDescriptor(outputTopic, new NoOpSerde<>());
    appDescriptor.withInputStream(inputDescriptor)
                 .withOutputStream(outputDescriptor)
                 .withTaskFactory((AsyncStreamTaskFactory) () -> new TestTaskImpl());
  }

  public interface TaskApplicationProcessCallback {
    void onMessage(IncomingMessageEnvelope m, TaskCallback callback);
  }
}
