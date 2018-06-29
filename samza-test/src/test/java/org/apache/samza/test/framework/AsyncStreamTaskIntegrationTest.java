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
import org.apache.samza.test.framework.stream.CollectionStream;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Test;
import scala.Int;


public class AsyncStreamTaskIntegrationTest {

  @Test
  public void testAsyncTaskWithSinglePartition() throws Exception {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputList = Arrays.asList(10, 20, 30, 40, 50);

    CollectionStream<Integer> input = CollectionStream.of("async-test", "ints", inputList);
    CollectionStream output = CollectionStream.empty("async-test", "ints-out");

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(input)
        .addOutputStream(output)
        .run(Duration.ofSeconds(2));

    Assert.assertThat(TestRunner.consumeStream(output, Duration.ofMillis(1000)).get(0),
        IsIterableContainingInOrder.contains(outputList.toArray()));
  }

  @Test
  public void testAsyncTaskWithSinglePartitionUsingStreamAssert() throws Exception {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputList = Arrays.asList(50, 10, 20, 30, 40);

    CollectionStream<Integer> input = CollectionStream.of("async-test", "ints", inputList);
    CollectionStream output = CollectionStream.empty("async-test", "ints-out");

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(input)
        .addOutputStream(output)
        .run(Duration.ofSeconds(2));

    StreamAssert.that(output).containsInAnyOrder(outputList, Duration.ofMillis(1000));
  }

  @Test
  public void testAsyncTaskWithMultiplePartition() throws Exception {
    Map<Integer, List<KV>> input = new HashMap<>();
    Map<Integer, List<Integer>> output = new HashMap<>();
    List<Integer> partition = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputPartition = partition.stream().map(x -> x * 10).collect(Collectors.toList());
    for(int i=0; i<5; i++){
      List<KV> keyedPartition = new ArrayList<>();
      for(Integer val: partition){ keyedPartition.add(KV.of(i, val)); }
      input.put(i, keyedPartition);
      output.put(i, new ArrayList<Integer>(outputPartition));
    }

    CollectionStream<KV> inputStream = CollectionStream.of("async-test", "ints", input);
    CollectionStream outputStream = CollectionStream.empty("async-test", "ints-out", 5);

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(inputStream)
        .addOutputStream(outputStream)
        .run(Duration.ofSeconds(2));

    StreamAssert.that(outputStream).contains(output, Duration.ofMillis(1000));
  }

  @Test
  public void testAsyncTaskWithMultiplePartitionMultithreaded() throws Exception {
    Map<Integer, List<KV>> input = new HashMap<>();
    Map<Integer, List<Integer>> output = new HashMap<>();
    List<Integer> partition = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputPartition = partition.stream().map(x -> x * 10).collect(Collectors.toList());
    for(int i=0; i<5; i++){
      List<KV> keyedPartition = new ArrayList<>();
      for(Integer val: partition){ keyedPartition.add(KV.of(i, val)); }
      input.put(i, keyedPartition);
      output.put(i, new ArrayList<Integer>(outputPartition));
    }

    CollectionStream<KV> inputStream = CollectionStream.of("async-test", "ints", input);
    CollectionStream outputStream = CollectionStream.empty("async-test", "ints-out", 5);

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(inputStream)
        .addOutputStream(outputStream)
        .addOverrideConfig("task.max.concurrency", "4")
        .run(Duration.ofSeconds(2));

    StreamAssert.that(outputStream).containsInAnyOrder(output, Duration.ofMillis(1000));
  }



  /**
   * Job should fail because it times out too soon
   */
  @Test(expected = AssertionError.class)
  public void testSamzaJobTimeoutFailureForAsyncTask() {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4);

    CollectionStream<Integer> input = CollectionStream.of("async-test", "ints", inputList);
    CollectionStream output = CollectionStream.empty("async-test", "ints-out");

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(input)
        .addOutputStream(output)
        .run(Duration.ofMillis(1));
  }
}
