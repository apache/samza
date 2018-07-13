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
import org.apache.samza.test.framework.stream.CollectionStream;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Test;


public class StreamTaskIntegrationTest {

  @Test
  public void testSyncTaskWithSinglePartition() throws Exception {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputList = Arrays.asList(10, 20, 30, 40, 50);

    CollectionStream<Integer> input = CollectionStream.of("test", "input", inputList);
    CollectionStream output = CollectionStream.empty("test", "output");

    TestRunner.of(MyStreamTestTask.class).addInputStream(input).addOutputStream(output).run(Duration.ofSeconds(1));

    Assert.assertThat(TestRunner.consumeStream(output, Duration.ofMillis(1000)).get(0),
        IsIterableContainingInOrder.contains(outputList.toArray()));
  }

  /**
   * Samza job logic expects integers, but doubles are passed here which results in failure
   */
  @Test(expected = SamzaException.class)
  public void testSamzaJobFailureForSyncTask() {
    List<Double> inputList = Arrays.asList(1.2, 2.3, 3.33, 4.5);

    CollectionStream<Double> input = CollectionStream.of("test", "doubles", inputList);
    CollectionStream output = CollectionStream.empty("test", "output");

    TestRunner.of(MyStreamTestTask.class).addInputStream(input).addOutputStream(output).run(Duration.ofSeconds(1));
  }

  @Test
  public void testSyncTaskWithSinglePartitionMultithreaded() throws Exception {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputList = Arrays.asList(10, 20, 30, 40, 50);

    CollectionStream<Integer> input = CollectionStream.of("test", "input", inputList);
    CollectionStream output = CollectionStream.empty("test", "output");

    TestRunner
        .of(MyStreamTestTask.class)
        .addInputStream(input)
        .addOutputStream(output)
        .addOverrideConfig("job.container.thread.pool.size", "4")
        .run(Duration.ofSeconds(1));

    StreamAssert.containsInOrder(output, outputList, Duration.ofMillis(1000));
  }

  @Test
  public void testSyncTaskWithMultiplePartition() throws Exception {
    Map<Integer, List<KV>> inputPartitionData = new HashMap<>();
    Map<Integer, List<Integer>> expectedOutputPartitionData = new HashMap<>();
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

    CollectionStream<KV> inputStream = CollectionStream.of("test", "input", inputPartitionData);
    CollectionStream outputStream = CollectionStream.empty("test", "output", 5);

    TestRunner
        .of(MyStreamTestTask.class)
        .addInputStream(inputStream)
        .addOutputStream(outputStream)
        .run(Duration.ofSeconds(2));

    StreamAssert.containsInOrder(outputStream, expectedOutputPartitionData, Duration.ofMillis(1000));
  }

  @Test
  public void testSyncTaskWithMultiplePartitionMultithreaded() throws Exception {
    Map<Integer, List<KV>> inputPartitionData = new HashMap<>();
    Map<Integer, List<Integer>> expectedOutputPartitionData = new HashMap<>();
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

    CollectionStream<KV> inputStream = CollectionStream.of("test", "input", inputPartitionData);
    CollectionStream outputStream = CollectionStream.empty("test", "output", 5);

    TestRunner
        .of(MyStreamTestTask.class)
        .addInputStream(inputStream)
        .addOutputStream(outputStream)
        .addOverrideConfig("job.container.thread.pool.size", "4")
        .run(Duration.ofSeconds(2));

    StreamAssert.containsInOrder(outputStream, expectedOutputPartitionData, Duration.ofMillis(1000));
  }
}
