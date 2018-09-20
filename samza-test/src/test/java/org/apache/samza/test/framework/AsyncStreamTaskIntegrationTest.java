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
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.system.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.InMemorySystemDescriptor;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Test;


public class AsyncStreamTaskIntegrationTest {

  @Test
  public void testAsyncTaskWithSinglePartition() throws Exception {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputList = Arrays.asList(10, 20, 30, 40, 50);

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("async-test");

    InMemoryInputDescriptor<Integer> imid = isd
        .getInputDescriptor("ints", new NoOpSerde<Integer>());

    InMemoryOutputDescriptor imod = isd
        .getOutputDescriptor("ints-out", new NoOpSerde<>());

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(imid, inputList)
        .addOutputStream(imod, 1)
        .run(Duration.ofSeconds(2));

    Assert.assertThat(TestRunner.consumeStream(imod, Duration.ofMillis(1000)).get(0),
        IsIterableContainingInOrder.contains(outputList.toArray()));
  }

  @Test
  public void testAsyncTaskWithSinglePartitionUsingStreamAssert() throws Exception {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputList = Arrays.asList(50, 10, 20, 30, 40);

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("async-test");

    InMemoryInputDescriptor<Integer> imid = isd
        .getInputDescriptor("ints", new NoOpSerde<Integer>());

    InMemoryOutputDescriptor imod = isd
        .getOutputDescriptor("ints-out", new NoOpSerde<>());

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(imid, inputList)
        .addOutputStream(imod, 1)
        .run(Duration.ofSeconds(2));

    StreamAssert.containsInAnyOrder(outputList, imod, Duration.ofMillis(1000));
  }

  @Test
  public void testAsyncTaskWithMultiplePartition() throws Exception {
    Map<Integer, List<KV>> inputPartitionData = new HashMap<>();
    Map<Integer, List<Integer>> expectedOutputPartitionData = new HashMap<>();
    genData(inputPartitionData, expectedOutputPartitionData);

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("async-test");

    InMemoryInputDescriptor<KV> imid = isd
        .getInputDescriptor("ints", new NoOpSerde<KV>());
    InMemoryOutputDescriptor imod = isd
        .getOutputDescriptor("ints-out", new NoOpSerde<>());

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(imid, inputPartitionData)
        .addOutputStream(imod, 5)
        .run(Duration.ofSeconds(2));

    StreamAssert.containsInOrder(expectedOutputPartitionData, imod, Duration.ofMillis(1000));
  }

  @Test
  public void testAsyncTaskWithMultiplePartitionMultithreaded() throws Exception {
    Map<Integer, List<KV>> inputPartitionData = new HashMap<>();
    Map<Integer, List<Integer>> expectedOutputPartitionData = new HashMap<>();
    genData(inputPartitionData, expectedOutputPartitionData);

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("async-test");

    InMemoryInputDescriptor<KV> imid = isd
        .getInputDescriptor("ints", new NoOpSerde<>());

    InMemoryOutputDescriptor imod = isd
        .getOutputDescriptor("ints-out", new NoOpSerde<>());

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(imid, inputPartitionData)
        .addOutputStream(imod, 5)
        .addOverrideConfig("task.max.concurrency", "4")
        .run(Duration.ofSeconds(2));

    StreamAssert.containsInAnyOrder(expectedOutputPartitionData, imod, Duration.ofMillis(1000));
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

  /**
   * Job should fail because it times out too soon
   */
  @Test(expected = AssertionError.class)
  public void testSamzaJobTimeoutFailureForAsyncTask() {
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("async-test");

    InMemoryInputDescriptor<Integer> imid = isd
        .getInputDescriptor("ints", new NoOpSerde<>());

    InMemoryOutputDescriptor imod = isd
        .getOutputDescriptor("ints-out", new NoOpSerde<>());

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(imid, Arrays.asList(1, 2, 3, 4))
        .addOutputStream(imod, 1)
        .run(Duration.ofMillis(1));
  }


}
