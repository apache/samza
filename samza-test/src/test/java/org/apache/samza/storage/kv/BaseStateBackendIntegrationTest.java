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

import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.samza.storage.MyStatefulApplication;
import org.apache.samza.storage.SideInputsProcessor;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.test.framework.StreamApplicationIntegrationTestHarness;
import org.apache.samza.util.FileUtil;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseStateBackendIntegrationTest extends StreamApplicationIntegrationTestHarness {
  private static final Logger LOG = LoggerFactory.getLogger(BaseStateBackendIntegrationTest.class);

  public void initialRun(
      String inputSystem,
      String inputTopicName,
      String sideInputTopicName,
      List<String> inputMessages,
      List<String> sideInputMessages,
      Set<String> regularStoreNames,
      Map<String, String> regularStoreChangelogTopics,
      Set<String> inMemoryStoreNames,
      Map<String, String> inMemoryStoreChangelogTopics,
      String sideInputStoreName,
      List<String> expectedChangelogMessagesAfterInitialRun,
      Map<String, String> overriddenConfigs) {
    // create input topic and produce the first batch of input messages
    createTopic(inputTopicName, 1);
    inputMessages.forEach(m -> produceMessage(inputTopicName, 0, m, m));

    // create side input topic and produce the first batch of side input messages
    createTopic(sideInputTopicName, 1);
    sideInputMessages.forEach(m -> produceMessage(sideInputTopicName, 0, m, m));


    // verify that the input messages were produced successfully
    if (inputMessages.size() > 0) {
      List<ConsumerRecord<String, String>> inputRecords =
          consumeMessages(inputTopicName, inputMessages.size());
      List<String> readInputMessages = inputRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
      Assert.assertEquals(inputMessages, readInputMessages);
    }

    // verify that the side input messages were produced successfully
    if (sideInputMessages.size() > 0) {
      List<ConsumerRecord<String, String>> sideInputRecords =
          consumeMessages(sideInputTopicName, sideInputMessages.size());
      List<String> readSideInputMessages = sideInputRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
      Assert.assertEquals(sideInputMessages, readSideInputMessages);
    }

    // run the application
    RunApplicationContext context = runApplication(
        new MyStatefulApplication(inputSystem, inputTopicName,
            regularStoreNames, regularStoreChangelogTopics,
            inMemoryStoreNames, inMemoryStoreChangelogTopics,
            Optional.of(sideInputStoreName), Optional.of(sideInputTopicName), Optional.of(new MySideInputProcessor())),
        "myApp", overriddenConfigs);

    // wait for the application to finish
    context.getRunner().waitForFinish();

    // consume and verify the changelog messages
    HashSet<String> changelogTopics = new HashSet<>(regularStoreChangelogTopics.values());
    changelogTopics.addAll(inMemoryStoreChangelogTopics.values());
    changelogTopics.forEach(changelogTopicName -> {
      if (expectedChangelogMessagesAfterInitialRun.size() > 0) {
        List<ConsumerRecord<String, String>> changelogRecords =
            consumeMessages(changelogTopicName, expectedChangelogMessagesAfterInitialRun.size());
        List<String> changelogMessages = changelogRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        Assert.assertEquals(expectedChangelogMessagesAfterInitialRun, changelogMessages);
      }
    });

    LOG.info("Finished initial run");
  }

  public void secondRun(
      boolean hostAffinity,
      String loggedStoreBaseDir,
      String inputSystem,
      String inputTopicName,
      String sideInputTopicName,
      List<String> inputMessages,
      List<String> sideInputMessages,
      Set<String> regularStoreNames,
      Map<String, String> regularStoreChangelogTopics,
      Set<String> inMemoryStoreNames,
      Map<String, String> inMemoryStoreChangelogTopics,
      String sideInputStoreName,
      List<String> expectedChangelogMessagesAfterSecondRun,
      List<String> expectedInitialStoreContents,
      List<String> expectedInitialInMemoryStoreContents,
      List<String> expectedInitialSideInputStoreContents,
      Map<String, String> overriddenConfigs) {
    // clear the local store directory
    if (!hostAffinity) {
      new FileUtil().rm(new File(loggedStoreBaseDir));
    }

    // produce the second batch of input messages

    inputMessages.forEach(m -> produceMessage(inputTopicName, 0, m, m));

    // produce the second batch of side input messages
    sideInputMessages.forEach(m -> produceMessage(sideInputTopicName, 0, m, m));

    // run the application
    RunApplicationContext context = runApplication(
        new MyStatefulApplication(inputSystem, inputTopicName,
            regularStoreNames, regularStoreChangelogTopics,
            inMemoryStoreNames, inMemoryStoreChangelogTopics,
            Optional.of(sideInputStoreName), Optional.of(sideInputTopicName), Optional.of(new MySideInputProcessor())),
        "myApp", overriddenConfigs);

    // wait for the application to finish
    context.getRunner().waitForFinish();

    // verify the store contents during startup
    for (String storeName: regularStoreNames) {
      Assert.assertEquals(expectedInitialStoreContents,
          MyStatefulApplication.getInitialStoreContents().get(storeName));
    }

    // verify the memory store contents during startup
    for (String storeName: inMemoryStoreNames) {
      Assert.assertEquals(expectedInitialInMemoryStoreContents,
          MyStatefulApplication.getInitialInMemoryStoreContents().get(storeName));
    }

    // verify that the side input store contents during startup include messages up to head of topic
    Assert.assertEquals(expectedInitialSideInputStoreContents,
        MyStatefulApplication.getInitialSideInputStoreContents().get(sideInputStoreName));

    // consume and verify any additional changelog messages for stores with changelogs
    HashSet<String> changelogTopics = new HashSet<>(regularStoreChangelogTopics.values());
    changelogTopics.addAll(inMemoryStoreChangelogTopics.values());
    changelogTopics.forEach(changelogTopicName -> {
      List<ConsumerRecord<String, String>> changelogRecords =
          consumeMessages(changelogTopicName, expectedChangelogMessagesAfterSecondRun.size());
      List<String> changelogMessages = changelogRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
      Assert.assertEquals(expectedChangelogMessagesAfterSecondRun, changelogMessages);
    });
  }

  static class MySideInputProcessor implements SideInputsProcessor, Serializable {
    @Override
    public Collection<Entry<?, ?>> process(IncomingMessageEnvelope message, KeyValueStore store) {
      return ImmutableSet.of(new Entry<>(message.getKey(), message.getMessage()));
    }
  }
}
