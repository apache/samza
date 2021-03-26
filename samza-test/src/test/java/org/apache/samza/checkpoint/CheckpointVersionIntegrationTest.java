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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.storage.MyStatefulApplication;
import org.apache.samza.test.framework.StreamApplicationIntegrationTestHarness;
import org.apache.samza.util.FileUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CheckpointVersionIntegrationTest extends StreamApplicationIntegrationTestHarness {

  private final static Logger LOG = LoggerFactory.getLogger(CheckpointVersionIntegrationTest.class);

  private static final String INPUT_TOPIC = "inputTopic";
  private static final String INPUT_SYSTEM = "kafka";
  private static final String STORE_NAME = "store";
  private static final String CHANGELOG_TOPIC = "changelog";
  private static final String LOGGED_STORE_BASE_DIR = new File(System.getProperty("java.io.tmpdir"), "logged-store").getAbsolutePath();
  private static final Map<String, String> CONFIGS = new HashMap<String, String>() { {
      put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
      put(JobConfig.PROCESSOR_ID, "0");
      put(TaskConfig.GROUPER_FACTORY, "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");
      put(TaskConfig.CHECKPOINT_MANAGER_FACTORY, "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory");
      put(TaskConfig.COMMIT_MS, "-1"); // manual commit only
      put(TaskConfig.TRANSACTIONAL_STATE_RESTORE_ENABLED, "true");
      put(TaskConfig.TRANSACTIONAL_STATE_RETAIN_EXISTING_STATE, "true");
      put(KafkaConfig.CHECKPOINT_REPLICATION_FACTOR(), "1");
      put(JobConfig.JOB_LOGGED_STORE_BASE_DIR, LOGGED_STORE_BASE_DIR);
    } };

  @Before
  @Override
  public void setUp() {
    super.setUp();
    // reset static state shared with task between each parameterized iteration
    MyStatefulApplication.resetTestState();
    new FileUtil().rm(new File(LOGGED_STORE_BASE_DIR)); // always clear local store on startup
  }

  @Test
  public void testStopCheckpointV1V2AndRestartCheckpointV2() {
    List<String> inputMessagesOnInitialRun = Arrays.asList("1", "2", "3", "2", "97", "-97", ":98", ":99", ":crash_once");
    // double check collectors.flush
    List<String> expectedChangelogMessagesOnInitialRun = Arrays.asList("1", "2", "3", "2", "97", null, "98", "99");
    initialRun(inputMessagesOnInitialRun, expectedChangelogMessagesOnInitialRun);

    // first two are reverts for uncommitted messages from last run for keys 98 and 99
    List<String> expectedChangelogMessagesAfterSecondRun =
        Arrays.asList(null, null, "98", "99", "4", "5", "5");
    List<String> expectedInitialStoreContentsOnSecondRun = Arrays.asList("1", "2", "3");
    Map<String, String> configOverrides = new HashMap<>(CONFIGS);
    configOverrides.put(TaskConfig.CHECKPOINT_READ_VERSION, "2");
    secondRun(CHANGELOG_TOPIC,
        expectedChangelogMessagesAfterSecondRun, expectedInitialStoreContentsOnSecondRun, configOverrides);
  }

  private void initialRun(List<String> inputMessages, List<String> expectedChangelogMessages) {
    // create input topic and produce the first batch of input messages
    createTopic(INPUT_TOPIC, 1);
    inputMessages.forEach(m -> produceMessage(INPUT_TOPIC, 0, m, m));

    // verify that the input messages were produced successfully
    if (inputMessages.size() > 0) {
      List<ConsumerRecord<String, String>> inputRecords =
          consumeMessages(Collections.singletonList(INPUT_TOPIC), inputMessages.size());
      List<String> readInputMessages = inputRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
      Assert.assertEquals(inputMessages, readInputMessages);
    }

    // run the application
    RunApplicationContext context = runApplication(
        new MyStatefulApplication(INPUT_SYSTEM, INPUT_TOPIC, Collections.singletonMap(STORE_NAME, CHANGELOG_TOPIC)), "myApp", CONFIGS);

    // wait for the application to finish
    context.getRunner().waitForFinish();

    // consume and verify the changelog messages
    if (expectedChangelogMessages.size() > 0) {
      List<ConsumerRecord<String, String>> changelogRecords =
          consumeMessages(Collections.singletonList(CHANGELOG_TOPIC), expectedChangelogMessages.size());
      List<String> changelogMessages = changelogRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
      Assert.assertEquals(expectedChangelogMessages, changelogMessages);
    }

    LOG.info("Finished initial run");
  }

  private void secondRun(String changelogTopic, List<String> expectedChangelogMessages,
      List<String> expectedInitialStoreContents, Map<String, String> overriddenConfigs) {
    // remove previous files so restore is from the checkpointV2
    new FileUtil().rm(new File(LOGGED_STORE_BASE_DIR));

    // produce the second batch of input messages

    List<String> inputMessages = Arrays.asList("4", "5", "5", ":shutdown");
    inputMessages.forEach(m -> produceMessage(INPUT_TOPIC, 0, m, m));

    // run the application
    RunApplicationContext context = runApplication(
        new MyStatefulApplication(INPUT_SYSTEM, INPUT_TOPIC, Collections.singletonMap(STORE_NAME, changelogTopic)), "myApp", overriddenConfigs);

    // wait for the application to finish
    context.getRunner().waitForFinish();

    // consume and verify any additional changelog messages
    List<ConsumerRecord<String, String>> changelogRecords =
        consumeMessages(Collections.singletonList(changelogTopic), expectedChangelogMessages.size());
    List<String> changelogMessages = changelogRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
    Assert.assertEquals(expectedChangelogMessages, changelogMessages);

    // verify the store contents during startup (this is after changelog verification to ensure init has completed)
    Assert.assertEquals(expectedInitialStoreContents, MyStatefulApplication.getInitialStoreContents().get(STORE_NAME));
  }
}
