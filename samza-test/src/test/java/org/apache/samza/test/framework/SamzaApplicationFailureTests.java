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
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.operators.KV;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.test.controlmessages.TestData;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.junit.Test;


public class SamzaApplicationFailureTests {
  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};

  final StreamApplication pageViewParition = (streamGraph, cfg) -> {
    streamGraph.<KV<String, TestData.PageView>>getInputStream("PageView").map(
        StreamApplicationIntegrationTest.Values.create())
        .partitionBy(pv -> pv.getMemberId(), pv -> pv, "p1")
        .sink((m, collector, coordinator) -> {
            collector.send(new OutgoingMessageEnvelope(new SystemStream("test", "Output"), m.getKey(), m.getKey(), m));
          });
  };

  final StreamApplication pageViewFilter = (streamGraph, cfg) -> {
    streamGraph.<KV<String, TestData.PageView>>getInputStream("PageView").map(
        StreamApplicationIntegrationTest.Values.create()).filter(pv -> pv.getPageKey().equals("inbox"));
  };

  /**
   * Samza job logic expects integers, but doubles are passed here which results in failure
   */
  @Test(expected = SamzaException.class)
  public void testSamzaJobFailureForSyncTask() throws TimeoutException {
    List<Double> inputList = Arrays.asList(1.2, 2.3, 3.33, 4.5);

    CollectionStream<Double> input = CollectionStream.of("test", "doubles", inputList);
    CollectionStream output = CollectionStream.empty("test", "output");

    TestRunner
        .of(MyStreamTestTask.class)
        .addInputStream(input)
        .addOutputStream(output)
        .run(Duration.ofSeconds(1));
  }

  /**
   * Job should fail because it times out too soon
   */
  @Test(expected = TimeoutException.class)
  public void testSamzaJobTimeoutFailureForAsyncTask() throws TimeoutException {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4);

    CollectionStream<Integer> input = CollectionStream.of("async-test", "ints", inputList);
    CollectionStream output = CollectionStream.empty("async-test", "ints-out");

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(input)
        .addOutputStream(output)
        .run(Duration.ofMillis(1));
  }

  /**
   * Job should fail since it is missing config "job.default.system" for partitionBy Operator
   */
  @Test(expected = SamzaException.class)
  public void testSamzaJobStartMissingConfigFailureForStreamApplication() throws TimeoutException {

    CollectionStream<TestData.PageView> input = CollectionStream.of("test", "PageView", new ArrayList<>());
    CollectionStream output = CollectionStream.empty("test", "Output", 10);

    TestRunner
        .of(pageViewParition)
        .addInputStream(input)
        .addOutputStream(output)
        .run(Duration.ofMillis(1000));
  }

  /**
   * Null page key is passed in input data which should fail filter logic
   */
  @Test(expected = SamzaException.class)
  public void testSamzaJobFailureForStreamApplication() throws TimeoutException {
    Random random = new Random();
    int count = 10;
    List<TestData.PageView> pageviews = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      int memberId = i;
      pageviews.add(new TestData.PageView(null, memberId));
    }

    CollectionStream<TestData.PageView> input = CollectionStream.of("test", "PageView", pageviews);
    CollectionStream output = CollectionStream.empty("test", "Output", 1);

    TestRunner.of(pageViewFilter)
        .addInputStream(input)
        .addOutputStream(output)
        .addOverrideConfig("job.default.system", "test")
        .run(Duration.ofMillis(1000));
  }
}
