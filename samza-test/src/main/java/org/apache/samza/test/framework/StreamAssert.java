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
import org.apache.samza.test.framework.stream.InMemoryOutputDescriptor;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;

import static org.junit.Assert.assertThat;


/**
 * Assertion utils non the content of a {@link org.apache.samza.operators.descriptors.base.stream.StreamDescriptor}.
 */
public class StreamAssert {
  /**
   * Util to assert  presence of messages in a stream with single partition in any order
   *
   * @param streamDescriptor represents the actual stream which will be consumed to compare against expected list
   * @param expected represents the expected stream of messages
   * @param timeout maximum time to wait for consuming the stream
   * @param <StreamMessageType> represents the type of Message in the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   */
  public static <StreamMessageType> void containsInAnyOrder(
      InMemoryOutputDescriptor<StreamMessageType> streamDescriptor, final List<StreamMessageType> expected,
      Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(streamDescriptor, "This util is intended to use only on streamDescriptor");
    assertThat(TestRunner.consumeStream(streamDescriptor, timeout)
        .entrySet()
        .stream()
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toList()), IsIterableContainingInAnyOrder.containsInAnyOrder(expected.toArray()));
  }

  /**
   * Util to assert presence of messages in a stream with multiple partition in any order
   *
   * @param streamDescriptor represents the actual stream which will be consumed to compare against expected partition map
   * @param expected represents a map of partitionId as key and list of messages in stream as value
   * @param timeout maximum time to wait for consuming the stream
   * @param <StreamMessageType> represents the type of Message in the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   *
   */
  public static <StreamMessageType> void containsInAnyOrder(
      InMemoryOutputDescriptor<StreamMessageType> streamDescriptor,
      final Map<Integer, List<StreamMessageType>> expected, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(streamDescriptor, "This util is intended to use only on streamDescriptor");
    Map<Integer, List<StreamMessageType>> actual = TestRunner.consumeStream(streamDescriptor, timeout);
    for (Integer paritionId : expected.keySet()) {
      assertThat(actual.get(paritionId),
          IsIterableContainingInAnyOrder.containsInAnyOrder(expected.get(paritionId).toArray()));
    }
  }

  /**
   * Util to assert ordering of messages in a stream with single partition
   *
   * @param streamDescriptor represents the actual stream which will be consumed to compare against expected list
   * @param expected represents the expected stream of messages
   * @param timeout maximum time to wait for consuming the stream
   * @param <StreamMessageType> represents the type of Message in the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   */
  public static <StreamMessageType> void containsInOrder(InMemoryOutputDescriptor<StreamMessageType> streamDescriptor,
      final List<StreamMessageType> expected, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(streamDescriptor, "This util is intended to use only on streamDescriptor");
    assertThat(TestRunner.consumeStream(streamDescriptor, timeout)
        .entrySet()
        .stream()
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toList()), IsIterableContainingInOrder.contains(expected.toArray()));
  }

  /**
   * Util to assert ordering of messages in a multi-partitioned stream
   *
   * @param streamDescriptor represents the actual stream which will be consumed to compare against expected partition map
   * @param expected represents a map of partitionId as key and list of messages as value
   * @param timeout maximum time to wait for consuming the stream
   * @param <StreamMessageType> represents the type of Message in the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   */
  public static <StreamMessageType> void containsInOrder(InMemoryOutputDescriptor<StreamMessageType> streamDescriptor,
      final Map<Integer, List<StreamMessageType>> expected, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(streamDescriptor, "This util is intended to use only on streamDescriptor");
    Map<Integer, List<StreamMessageType>> actual = TestRunner.consumeStream(streamDescriptor, timeout);
    for (Integer paritionId : expected.keySet()) {
      assertThat(actual.get(paritionId), IsIterableContainingInOrder.contains(expected.get(paritionId).toArray()));
    }
  }
}
