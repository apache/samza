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

package org.apache.samza.test.framework;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.context.Context;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.table.TestTableData;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.test.table.TestTableData.EnrichedPageView;
import static org.apache.samza.test.table.TestTableData.PageView;
import static org.apache.samza.test.table.TestTableData.Profile;

public class StreamTaskIntegrationTest {

  @Test
  public void testStatefulTaskWithLocalTable() {
    List<PageView> pageViews = Arrays.asList(TestTableData.generatePageViews(10));
    List<Profile> profiles = Arrays.asList(TestTableData.generateProfiles(10));

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<TestTableData.PageView> pageViewStreamDesc = isd
        .getInputDescriptor("PageView", new NoOpSerde<TestTableData.PageView>());

    InMemoryInputDescriptor<TestTableData.Profile> profileStreamDesc = isd
        .getInputDescriptor("Profile", new NoOpSerde<TestTableData.Profile>())
        .shouldBootstrap();

    InMemoryOutputDescriptor<TestTableData.EnrichedPageView> outputStreamDesc = isd
        .getOutputDescriptor("EnrichedPageView", new NoOpSerde<>());

    TestRunner
        .of(new JoinTaskApplication())
        .addInputStream(pageViewStreamDesc, pageViews)
        .addInputStream(profileStreamDesc, profiles)
        .addOutputStream(outputStreamDesc, 1)
        .run(Duration.ofSeconds(2));

    Assert.assertEquals(10, TestRunner.consumeStream(outputStreamDesc, Duration.ofSeconds(1)).get(0).size());
  }

  @Test
  public void testSyncTaskWithSinglePartition() throws Exception {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputList = Arrays.asList(10, 20, 30, 40, 50);

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<Integer> imid = isd
        .getInputDescriptor("input", new NoOpSerde<Integer>());

    InMemoryOutputDescriptor<Integer> imod = isd
        .getOutputDescriptor("output", new NoOpSerde<Integer>());

    TestRunner
        .of(MyStreamTestTask.class)
        .addInputStream(imid, inputList)
        .addOutputStream(imod, 1)
        .addExternalContext(new TestContext(10))
        .run(Duration.ofSeconds(1));

    Assert.assertThat(TestRunner.consumeStream(imod, Duration.ofMillis(1000)).get(0),
        IsIterableContainingInOrder.contains(outputList.toArray()));

  }

  /**
   * Samza job logic expects integers, but doubles are passed here which results in failure
   */
  @Test(expected = SamzaException.class)
  public void testSamzaJobFailureForSyncTask() {
    List<Double> inputList = Arrays.asList(1.2, 2.3, 3.33, 4.5);

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<Double> imid = isd
        .getInputDescriptor("doubles", new NoOpSerde<Double>());

    InMemoryOutputDescriptor imod = isd
        .getOutputDescriptor("output", new NoOpSerde<>());

    TestRunner
        .of(MyStreamTestTask.class)
        .addInputStream(imid, inputList)
        .addOutputStream(imod, 1)
        .addExternalContext(new TestContext(10))
        .run(Duration.ofSeconds(1));
  }

  @Test
  public void testSyncTaskWithSinglePartitionMultithreaded() throws Exception {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputList = Arrays.asList(10, 20, 30, 40, 50);

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<Integer> imid = isd
        .getInputDescriptor("input", new NoOpSerde<Integer>());

    InMemoryOutputDescriptor<Integer> imod = isd
        .getOutputDescriptor("output", new NoOpSerde<Integer>());

    TestRunner
        .of(MyStreamTestTask.class)
        .addInputStream(imid, inputList)
        .addOutputStream(imod, 1)
        .addConfig("job.container.thread.pool.size", "4")
        .addExternalContext(new TestContext(10))
        .run(Duration.ofSeconds(1));

    StreamAssert.containsInOrder(outputList, imod, Duration.ofMillis(1000));
  }

  @Test
  public void testSyncTaskWithMultiplePartition() throws Exception {
    Map<Integer, List<KV>> inputPartitionData = new HashMap<>();
    Map<Integer, List<Integer>> expectedOutputPartitionData = new HashMap<>();
    genData(inputPartitionData, expectedOutputPartitionData);

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<KV> imid = isd
        .getInputDescriptor("input", new NoOpSerde<KV>());

    InMemoryOutputDescriptor<Integer> imod = isd
        .getOutputDescriptor("output", new NoOpSerde<Integer>());

    TestRunner
        .of(MyStreamTestTask.class)
        .addInputStream(imid, inputPartitionData)
        .addOutputStream(imod, 5)
        .addExternalContext(new TestContext(10))
        .run(Duration.ofSeconds(2));

    StreamAssert.containsInOrder(expectedOutputPartitionData, imod, Duration.ofMillis(1000));
  }

  @Test
  public void testSyncTaskWithMultiplePartitionMultithreaded() throws Exception {
    Map<Integer, List<KV>> inputPartitionData = new HashMap<>();
    Map<Integer, List<Integer>> expectedOutputPartitionData = new HashMap<>();
    genData(inputPartitionData, expectedOutputPartitionData);

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");

    InMemoryInputDescriptor<KV> imid = isd
        .getInputDescriptor("input", new NoOpSerde<KV>());

    InMemoryOutputDescriptor<Integer> imod = isd
        .getOutputDescriptor("output", new NoOpSerde<Integer>());

    TestRunner
        .of(MyStreamTestTask.class)
        .addInputStream(imid, inputPartitionData)
        .addOutputStream(imod, 5)
        .addConfig("job.container.thread.pool.size", "4")
        .addExternalContext(new TestContext(10))
        .run(Duration.ofSeconds(2));

    StreamAssert.containsInOrder(expectedOutputPartitionData, imod, Duration.ofMillis(1000));
  }

  static public class JoinTaskApplication implements TaskApplication {
    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {
      KafkaSystemDescriptor ksd = new KafkaSystemDescriptor("test");
      KafkaInputDescriptor<Profile> profileISD = ksd.getInputDescriptor("Profile", new JsonSerdeV2<>());
      KafkaInputDescriptor<PageView> pageViewISD = ksd.getInputDescriptor("PageView", new JsonSerdeV2<>());
      KafkaOutputDescriptor<EnrichedPageView> enrichedPageViewOSD =
          ksd.getOutputDescriptor("EnrichedPageView", new NoOpSerde<>());
      appDescriptor
          .withInputStream(profileISD)
          .withInputStream(pageViewISD)
          .withOutputStream(enrichedPageViewOSD)
          .withTable(new InMemoryTableDescriptor("profile-view-store",
              KVSerde.of(new IntegerSerde(), new TestTableData.ProfileJsonSerde())))
          .withTaskFactory((StreamTaskFactory) () -> new StatefulStreamTask());
    }
  }

  static public class StatefulStreamTask implements StreamTask, InitableTask {
    private ReadWriteTable<Integer, Profile> profileViewTable;

    @Override
    public void init(Context context) throws Exception {
      profileViewTable = context.getTaskContext().getTable("profile-view-store");
    }

    @Override
    public void process(IncomingMessageEnvelope message, MessageCollector collector, TaskCoordinator coordinator) {
      if (message.getMessage() instanceof Profile) {
        Profile profile = (Profile) message.getMessage();
        profileViewTable.put(profile.getMemberId(), profile);
      } else if (message.getMessage() instanceof PageView) {
        PageView pageView = (PageView) message.getMessage();
        Profile profile = profileViewTable.get(pageView.getMemberId());
        if (profile != null) {
          System.out.println("Joining Page View ArticleView by " + profile.getMemberId());
          collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "EnrichedPageView"), null, null,
              new TestTableData.EnrichedPageView(pageView.getPageKey(), pageView.getMemberId(), profile.getCompany())));
        }
      }
    }

  }

  public void genData(Map<Integer, List<KV>> inputPartitionData, Map<Integer, List<Integer>> expectedOutputPartitionData) {
    List<Integer> partition = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputPartition = partition.stream().map(x -> x * 10).collect(Collectors.toList());
    for (int i = 0; i < 5; i++) {
      List<KV> keyedPartition = new ArrayList<>();
      for (Integer val : partition) {
        keyedPartition.add(KV.of(i, val));
      }
      inputPartitionData.put(i, keyedPartition);
      expectedOutputPartitionData.put(i, new ArrayList<Integer>(outputPartition));
    }
  }

}

class TestContext implements ExternalContext {
  final private int multiplier;

  public TestContext(int multiplier) {
    this.multiplier = multiplier;
  }

  public int getMultiplier() {
    return this.multiplier;
  }
}
