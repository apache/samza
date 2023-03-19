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

package org.apache.samza.storage.kv;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.storage.MyStatefulApplication;
import org.apache.samza.util.FileUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


@RunWith(value = Parameterized.class)
public class KafkaNonTransactionalStateIntegrationTest extends BaseStateBackendIntegrationTest {
  @Parameterized.Parameters(name = "hostAffinity={0}")
  public static Collection<Boolean> data() {
    return Arrays.asList(true, false);
  }

  private static final String INPUT_SYSTEM = "kafka";
  private static final String INPUT_TOPIC = "inputTopic";
  private static final String SIDE_INPUT_TOPIC = "sideInputTopic";

  private static final String REGULAR_STORE_NAME = "regularStore";
  private static final String REGULAR_STORE_CHANGELOG_TOPIC = "changelog";
  private static final String IN_MEMORY_STORE_NAME = "inMemoryStore";
  private static final String IN_MEMORY_STORE_CHANGELOG_TOPIC = "inMemoryStoreChangelog";
  private static final String SIDE_INPUT_STORE_NAME = "sideInputStore";

  private static final String LOGGED_STORE_BASE_DIR = new File(System.getProperty("java.io.tmpdir"), "logged-store").getAbsolutePath();

  private static final Map<String, String> CONFIGS = new HashMap<String, String>() { {
      put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
      put(JobConfig.PROCESSOR_ID, "0");
      put(TaskConfig.GROUPER_FACTORY, "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");

      put(TaskConfig.CHECKPOINT_MANAGER_FACTORY, "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory");
      put(KafkaConfig.CHECKPOINT_REPLICATION_FACTOR(), "1");

      put(TaskConfig.COMMIT_MS, "-1"); // manual commit only
      put(TaskConfig.COMMIT_MAX_DELAY_MS, "0"); // Ensure no commits are skipped due to in progress commits

      put(TaskConfig.TRANSACTIONAL_STATE_RESTORE_ENABLED, "false");
      put(TaskConfig.TRANSACTIONAL_STATE_RETAIN_EXISTING_STATE, "false");
      put(JobConfig.JOB_LOGGED_STORE_BASE_DIR, LOGGED_STORE_BASE_DIR);
    } };

  private final boolean hostAffinity;

  public KafkaNonTransactionalStateIntegrationTest(boolean hostAffinity) {
    this.hostAffinity = hostAffinity;
  }

  @Before
  @Override
  public void setUp() {
    super.setUp();
    // reset static state shared with task between each parameterized iteration
    MyStatefulApplication.resetTestState();
    new FileUtil().rm(new File(LOGGED_STORE_BASE_DIR)); // always clear local store on startup
  }

  @Test
  public void testStopAndRestart() {
    List<String> inputMessagesOnInitialRun = Arrays.asList("1", "2", "3", "2", "97", "-97", ":98", ":99", ":crash_once");
    List<String> sideInputMessagesOnInitialRun = Arrays.asList("1", "2", "3", "4", "5", "6");
    List<String> expectedChangelogMessagesAfterInitialRun = Arrays.asList("1", "2", "3", "2", "97", null, "98", "99");
    initialRun(
        INPUT_SYSTEM,
        INPUT_TOPIC,
        SIDE_INPUT_TOPIC,
        inputMessagesOnInitialRun,
        sideInputMessagesOnInitialRun,
        ImmutableSet.of(REGULAR_STORE_NAME),
        ImmutableMap.of(REGULAR_STORE_NAME, REGULAR_STORE_CHANGELOG_TOPIC),
        ImmutableSet.of(IN_MEMORY_STORE_NAME),
        ImmutableMap.of(IN_MEMORY_STORE_NAME, IN_MEMORY_STORE_CHANGELOG_TOPIC),
        SIDE_INPUT_STORE_NAME,
        expectedChangelogMessagesAfterInitialRun,
        CONFIGS);

    // first two are reverts for uncommitted messages from last run for keys 98 and 99
    List<String> expectedChangelogMessagesAfterSecondRun =
        Arrays.asList("98", "99", "4", "5", "5");
    List<String> inputMessagesBeforeSecondRun = Arrays.asList("4", "5", "5", ":shutdown");
    List<String> sideInputMessagesBeforeSecondRun = Arrays.asList("7", "8", "9");
    List<String> expectedInitialStoreContentsOnSecondRun = Arrays.asList("1", "2", "3", "98", "99");
    List<String> expectedInitialInMemoryStoreContentsOnSecondRun = Arrays.asList("1", "2", "3", "98", "99");
    List<String> expectedInitialSideInputStoreContentsOnSecondRun = new ArrayList<>(sideInputMessagesOnInitialRun);
    expectedInitialSideInputStoreContentsOnSecondRun.addAll(sideInputMessagesBeforeSecondRun);
    secondRun(
        hostAffinity,
        LOGGED_STORE_BASE_DIR,
        INPUT_SYSTEM,
        INPUT_TOPIC,
        SIDE_INPUT_TOPIC,
        inputMessagesBeforeSecondRun,
        sideInputMessagesBeforeSecondRun,
        ImmutableSet.of(REGULAR_STORE_NAME),
        ImmutableMap.of(REGULAR_STORE_NAME, REGULAR_STORE_CHANGELOG_TOPIC),
        ImmutableSet.of(IN_MEMORY_STORE_NAME),
        ImmutableMap.of(IN_MEMORY_STORE_NAME, IN_MEMORY_STORE_CHANGELOG_TOPIC),
        SIDE_INPUT_STORE_NAME,
        expectedChangelogMessagesAfterSecondRun,
        expectedInitialStoreContentsOnSecondRun,
        expectedInitialInMemoryStoreContentsOnSecondRun,
        expectedInitialSideInputStoreContentsOnSecondRun,
        CONFIGS);
  }
}