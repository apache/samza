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

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;
import org.apache.samza.test.framework.system.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.TestRunner;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;

import static org.junit.Assert.assertThat;


/**
 * Assertion utils on the content of a stream described by
 * {@link org.apache.samza.operators.descriptors.base.stream.StreamDescriptor}.
 */
public class StreamAssert {
  /**
   * Verifies that the {@code expected} messages are present in any order in the single partition stream
   * represented by {@code outputDescriptor}
   *
   * @param expected expected stream of messages
   * @param outputDescriptor describes the stream which will be consumed to compare against expected list
   * @param timeout maximum time to wait for consuming the stream
   * @param <StreamMessageType> type of messages in the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   */
  public static <StreamMessageType> void containsInAnyOrder(List<StreamMessageType> expected,
      InMemoryOutputDescriptor<StreamMessageType> outputDescriptor, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(outputDescriptor);
    assertThat(TestRunner.consumeStream(outputDescriptor, timeout)
        .entrySet()
        .stream()
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toList()), IsIterableContainingInAnyOrder.containsInAnyOrder(expected.toArray()));
  }

  /**
   * Verifies that the {@code expected} messages are present in any order in the multi partition stream
   * represented by {@code outputDescriptor}
   *
   * @param expected map of partitionId as key and list of messages in stream as value
   * @param outputDescriptor describes the stream which will be consumed to compare against expected partition map
   * @param timeout maximum time to wait for consuming the stream
   * @param <StreamMessageType>  type of messages in the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   *
   */
  public static <StreamMessageType> void containsInAnyOrder(Map<Integer, List<StreamMessageType>> expected,
      InMemoryOutputDescriptor<StreamMessageType> outputDescriptor, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(outputDescriptor);
    Map<Integer, List<StreamMessageType>> actual = TestRunner.consumeStream(outputDescriptor, timeout);
    for (Integer paritionId : expected.keySet()) {
      assertThat(actual.get(paritionId),
          IsIterableContainingInAnyOrder.containsInAnyOrder(expected.get(paritionId).toArray()));
    }
  }

  /**
   * Verifies that the {@code expected} messages are present in order in the single partition stream
   * represented by {@code outputDescriptor}
   *
   * @param expected  expected stream of messages
   * @param outputDescriptor describes the stream which will be consumed to compare against expected list
   * @param timeout maximum time to wait for consuming the stream
   * @param <StreamMessageType> type of messages in the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   */
  public static <StreamMessageType> void containsInOrder(List<StreamMessageType> expected,
      InMemoryOutputDescriptor<StreamMessageType> outputDescriptor, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(outputDescriptor);
    assertThat(TestRunner.consumeStream(outputDescriptor, timeout)
        .entrySet()
        .stream()
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toList()), IsIterableContainingInOrder.contains(expected.toArray()));
  }

  /**
   * Verifies that the {@code expected} messages are present in order in the multi partition stream
   * represented by {@code outputDescriptor}
   *
   * @param expected map of partitionId as key and list of messages as value
   * @param outputDescriptor describes the stream which will be consumed to compare against expected partition map
   * @param timeout maximum time to wait for consuming the stream
   * @param <StreamMessageType> type of messages in the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   */
  public static <StreamMessageType> void containsInOrder(Map<Integer, List<StreamMessageType>> expected,
      InMemoryOutputDescriptor<StreamMessageType> outputDescriptor, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(outputDescriptor);
    Map<Integer, List<StreamMessageType>> actual = TestRunner.consumeStream(outputDescriptor, timeout);
    for (Integer paritionId : expected.keySet()) {
      assertThat(actual.get(paritionId), IsIterableContainingInOrder.contains(expected.get(paritionId).toArray()));
    }
  }
}
