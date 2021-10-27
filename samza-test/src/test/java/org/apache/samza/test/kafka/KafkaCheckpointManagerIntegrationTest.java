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
package org.apache.samza.test.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.StreamApplicationIntegrationTestHarness;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


/**
 * 1) Run app and consume messages
 * 2) Commit only for first message
 * 3) Shutdown application
 * 4) Run app a second time to use the checkpoint
 * 5) Verify that we had to re-process the message after the first message
 */
public class KafkaCheckpointManagerIntegrationTest extends StreamApplicationIntegrationTestHarness {
  private static final String SYSTEM = "kafka";
  private static final String INPUT_STREAM = "inputStream";
  private static final Map<String, String> CONFIGS = ImmutableMap.of(
      JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.standalone.PassthroughJobCoordinatorFactory",
      JobConfig.PROCESSOR_ID, "0",
      TaskConfig.CHECKPOINT_MANAGER_FACTORY, "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory",
      KafkaConfig.CHECKPOINT_REPLICATION_FACTOR(), "1",
      TaskConfig.COMMIT_MS, "-1"); // manual commit only
  /**
   * Keep track of which messages have been received by the application.
   */
  private static final Map<String, AtomicInteger> PROCESSED = new HashMap<>();

  /**
   * If message has this prefix, then request a commit after processing it.
   */
  private static final String COMMIT_PREFIX = "commit";
  /**
   * If message equals this string, then shut down the task if the task is configured to handle intermediate shutdown.
   */
  private static final String INTERMEDIATE_SHUTDOWN = "intermediateShutdown";
  /**
   * If message equals this string, then shut down the task.
   */
  private static final String END_OF_STREAM = "endOfStream";

  @Before
  public void setup() {
    PROCESSED.clear();
  }

  @Test
  public void testCheckpoint() {
    createTopic(INPUT_STREAM, 2);
    produceMessages(0);
    produceMessages(1);

    // run application once and verify processed messages before shutdown
    runApplication(new CheckpointApplication(true), "CheckpointApplication", CONFIGS).getRunner().waitForFinish();
    verifyProcessedMessagesFirstRun();

    // run application a second time and verify that certain messages had to be re-processed
    runApplication(new CheckpointApplication(false), "CheckpointApplication", CONFIGS).getRunner().waitForFinish();
    verifyProcessedMessagesSecondRun();
  }

  private void produceMessages(int partitionId) {
    String key = "key" + partitionId;
    // commit first message
    produceMessage(INPUT_STREAM, partitionId, key, commitMessage(partitionId, 0));
    // don't commit second message
    produceMessage(INPUT_STREAM, partitionId, key, noCommitMessage(partitionId, 1));
    // do an initial shutdown so that the test can check that the second message gets re-processed
    produceMessage(INPUT_STREAM, partitionId, key, INTERMEDIATE_SHUTDOWN);
    // do a commit on the third message
    produceMessage(INPUT_STREAM, partitionId, key, commitMessage(partitionId, 2));
    // this will make the task shut down for the second run
    produceMessage(INPUT_STREAM, partitionId, key, END_OF_STREAM);
  }

  /**
   * Each partition should have seen two messages before shutting down.
   */
  private static void verifyProcessedMessagesFirstRun() {
    assertEquals(4, PROCESSED.size());
    assertEquals(1, PROCESSED.get(commitMessage(0, 0)).get());
    assertEquals(1, PROCESSED.get(noCommitMessage(0, 1)).get());
    assertEquals(1, PROCESSED.get(commitMessage(1, 0)).get());
    assertEquals(1, PROCESSED.get(noCommitMessage(1, 1)).get());
  }

  /**
   * For each partition: re-process the second message (for 2 total of the second message), receive the third message.
   */
  private static void verifyProcessedMessagesSecondRun() {
    assertEquals(6, PROCESSED.size());
    assertEquals(1, PROCESSED.get(commitMessage(0, 0)).get());
    assertEquals(2, PROCESSED.get(noCommitMessage(0, 1)).get());
    assertEquals(1, PROCESSED.get(commitMessage(0, 2)).get());
    assertEquals(1, PROCESSED.get(commitMessage(1, 0)).get());
    assertEquals(2, PROCESSED.get(noCommitMessage(1, 1)).get());
    assertEquals(1, PROCESSED.get(commitMessage(1, 2)).get());
  }

  private static String commitMessage(int partitionId, int messageId) {
    return String.join("_", COMMIT_PREFIX, "partition", Integer.toString(partitionId), Integer.toString(messageId));
  }

  private static String noCommitMessage(int partitionId, int messageId) {
    return String.join("_", "partition", Integer.toString(partitionId), Integer.toString(messageId));
  }

  private static class CheckpointApplication implements TaskApplication {
    private final boolean handleIntermediateShutdown;

    private CheckpointApplication(boolean handleIntermediateShutdown) {
      this.handleIntermediateShutdown = handleIntermediateShutdown;
    }

    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {
      KafkaSystemDescriptor sd = new KafkaSystemDescriptor(SYSTEM);
      KafkaInputDescriptor<String> isd = sd.getInputDescriptor(INPUT_STREAM, new StringSerde());
      appDescriptor.withInputStream(isd)
          .withTaskFactory((StreamTaskFactory) () -> new CheckpointTask(this.handleIntermediateShutdown));
    }
  }

  private static class CheckpointTask implements StreamTask {
    /**
     * Determine if task should respond to {@link #INTERMEDIATE_SHUTDOWN}.
     * Helps with testing that any uncommitted messages get reprocessed if the job starts again.
     */
    private final boolean handleIntermediateShutdown;
    /**
     * When requesting shutdown, there is no guarantee of an immediate shutdown, since there are multiple tasks in the
     * container. Use this flag to make sure we don't process more messages past the shutdown request in order to have
     * deterministic counting of the messages for the test.
     */
    private boolean stopProcessing = false;

    private CheckpointTask(boolean handleIntermediateShutdown) {
      this.handleIntermediateShutdown = handleIntermediateShutdown;
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
      if (!this.stopProcessing) {
        String value = (String) envelope.getMessage();
        if (INTERMEDIATE_SHUTDOWN.equals(value)) {
          if (this.handleIntermediateShutdown) {
            setShutdown(coordinator);
          }
        } else if (END_OF_STREAM.equals(value)) {
          setShutdown(coordinator);
        } else {
          synchronized (this) {
            PROCESSED.putIfAbsent(value, new AtomicInteger(0));
            PROCESSED.get(value).incrementAndGet();
          }
          if (value.startsWith(COMMIT_PREFIX)) {
            coordinator.commit(TaskCoordinator.RequestScope.CURRENT_TASK);
          }
        }
      }
    }

    private void setShutdown(TaskCoordinator coordinator) {
      this.stopProcessing = true;
      coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
    }
  }
}
