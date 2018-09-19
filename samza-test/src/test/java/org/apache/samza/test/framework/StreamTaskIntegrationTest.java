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
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.system.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.InMemorySystemDescriptor;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Test;


public class StreamTaskIntegrationTest {

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
        .addOverrideConfig("job.container.thread.pool.size", "4")
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
        .addOverrideConfig("job.container.thread.pool.size", "4")
        .run(Duration.ofSeconds(2));

    StreamAssert.containsInOrder(expectedOutputPartitionData, imod, Duration.ofMillis(1000));
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
