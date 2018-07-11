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
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.stream.Collectors;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.SinkFunction;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.collection.IsIterableContainingInOrder;

import static org.junit.Assert.assertThat;

/**
 * An assertion on the content of a {@link MessageStream}.
 *
 * <pre>Example: {@code
 * MessageStream<String> stream = streamGraph.getInputStream("input", serde).map(some_function)...;
 * ...
 * StreamAssert.that(id, stream, stringSerde).containsInAnyOrder(Arrays.asList("a", "b", "c"));
 * }</pre>
 *
 */
public class StreamAssert<M> {
  private final static Map<String, CountDownLatch> LATCHES = new ConcurrentHashMap<>();
  private final static CountDownLatch PLACE_HOLDER = new CountDownLatch(0);

  private final String id;
  private final MessageStream<M> messageStream;
  private final CollectionStream<M> collectionStream;
  private final Serde<M> serde;
  private boolean checkEachTask = false;

  public static <M> StreamAssert<M> that(String id, MessageStream<M> messageStream, Serde<M> serde) {
    return new StreamAssert<>(id, messageStream, serde);
  }

  public static <M> StreamAssert<M> that(CollectionStream<M> collectionStream) {
    return new StreamAssert<>(collectionStream);
  }

  private StreamAssert(String id, MessageStream<M> messageStream, Serde<M> serde) {
    this.id = id;
    this.messageStream = messageStream;
    this.serde = serde;
    this.collectionStream = null;
  }

  private StreamAssert(CollectionStream<M> collectionStream) {
    this.id = null;
    this.messageStream = null;
    this.serde = null;
    this.collectionStream = collectionStream;
  }

  public StreamAssert forEachTask() {
    checkEachTask = true;
    return this;
  }

  public void containsInAnyOrder(final Collection<M> expected) {
    Preconditions.checkNotNull(messageStream, "This util is intended to use only on MessageStream");
    LATCHES.putIfAbsent(id, PLACE_HOLDER);
    final MessageStream<M> streamToCheck = checkEachTask
        ? messageStream
        : messageStream
          .partitionBy(m -> null, m -> m, KVSerde.of(new StringSerde(), serde), null)
          .map(kv -> kv.value);

    streamToCheck.sink(new CheckAgainstExpected<M>(id, expected, checkEachTask, Matcher.CONTAINS_IN_ANY_ORDER));
  }

  public void contains(final Collection<M> expected) {
    LATCHES.putIfAbsent(id, PLACE_HOLDER);
    final MessageStream<M> streamToCheck = checkEachTask
        ? messageStream
        : messageStream
            .partitionBy(m -> null, m -> m, KVSerde.of(new StringSerde(), serde), null)
            .map(kv -> kv.value);

    streamToCheck.sink(new CheckAgainstExpected<M>(id, expected, checkEachTask, Matcher.CONTAINS_IN_ORDER));
  }

  public static void waitForComplete() {
    try {
      while (!LATCHES.isEmpty()) {
        final Set<String> ids  = new HashSet<>(LATCHES.keySet());
        for (String id : ids) {
          while (LATCHES.get(id) == PLACE_HOLDER) {
            Thread.sleep(100);
          }

          final CountDownLatch latch = LATCHES.get(id);
          if (latch != null) {
            latch.await();
            LATCHES.remove(id);
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private enum Matcher {
    CONTAINS_IN_ANY_ORDER,
    CONTAINS_IN_ORDER
  }

  private static final class CheckAgainstExpected<M> implements SinkFunction<M> {
    private static final long TIMEOUT = 5000L;

    private final String id;
    private final boolean checkEachTask;
    private final Collection<M> expected;

    private transient Timer timer = new Timer();
    private Matcher matcher;
    private transient List<M> actual = Collections.synchronizedList(new ArrayList<>());
    private transient TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        check();
      }
    };

    CheckAgainstExpected(String id, Collection<M> expected, boolean checkEachTask, Matcher matcher) {
      this.id = id;
      this.expected = expected;
      this.checkEachTask = checkEachTask;
      this.matcher = matcher;
    }

    @Override
    public void init(Config config, TaskContext context) {
      final SystemStreamPartition ssp = Iterables.getFirst(context.getSystemStreamPartitions(), null);
      if (ssp == null ? false : ssp.getPartition().getPartitionId() == 0) {
        final int count = checkEachTask ? context.getSamzaContainerContext().taskNames.size() : 1;
        LATCHES.put(id, new CountDownLatch(count));
        timer.schedule(timerTask, TIMEOUT);
      }
    }

    @Override
    public void apply(M message, MessageCollector messageCollector, TaskCoordinator taskCoordinator) {
      actual.add(message);

      if (actual.size() >= expected.size()) {
        timerTask.cancel();
        check();
      }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      timer = new Timer();
      actual = Collections.synchronizedList(new ArrayList<>());
      timerTask = new TimerTask() {
        @Override
        public void run() {
          check();
        }
      };
    }

    private void check() {
      final CountDownLatch latch = LATCHES.get(id);
      try {
        if (matcher == Matcher.CONTAINS_IN_ANY_ORDER) {
          assertThat(actual, Matchers.containsInAnyOrder((M[]) expected.toArray()));
        } else if (matcher == Matcher.CONTAINS_IN_ORDER) {
          assertThat(actual, Matchers.contains((M[]) expected.toArray()));
        }
      } finally {
        latch.countDown();
      }
    }
  }

  /**
   * Util to assert  presence of messages in a stream with single partition in any order
   *
   * @param expected represents the expected stream of messages
   * @param timeout maximum time to wait for consuming the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   *
   */
  public void containsInAnyOrder(final List<M> expected, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(collectionStream,
        "This util is intended to use only on CollectionStream)");
    assertThat(TestRunner.consumeStream(collectionStream, timeout)
        .entrySet()
        .stream()
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toList()), IsIterableContainingInAnyOrder.containsInAnyOrder(expected.toArray()));
  }

  /**
   * Util to assert presence of messages in a stream with multiple partition in any order
   *
   * @param expected represents a map of partitionId as key and list of messages in stream as value
   * @param timeout maximum time to wait for consuming the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   *
   */
  public void containsInAnyOrder(final Map<Integer, List<M>> expected, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(collectionStream,
        "This util is intended to use only on CollectionStream)");
    Map<Integer, List<M>> actual = TestRunner.consumeStream(collectionStream, timeout);
    for (Integer paritionId: expected.keySet()) {
      assertThat(actual.get(paritionId), IsIterableContainingInAnyOrder.containsInAnyOrder(expected.get(paritionId).toArray()));
    }
  }

  /**
   * Util to assert ordering of messages in a stream with single partition
   *
   * @param expected represents the expected stream of messages
   * @param timeout maximum time to wait for consuming the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   */
  public void contains(final List<M> expected, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(collectionStream,
        "This util is intended to use only on CollectionStream)");
    assertThat(TestRunner.consumeStream(collectionStream, timeout)
        .entrySet()
        .stream()
        .flatMap(entry -> entry.getValue().stream())
        .collect(Collectors.toList()), IsIterableContainingInOrder.contains(expected.toArray()));
  }

  /**
   * Util to assert ordering of messages in a multi-partitioned stream
   *
   * @param expected represents a map of partitionId as key and list of messages as value
   * @param timeout maximum time to wait for consuming the stream
   * @throws InterruptedException when {@code consumeStream} is interrupted by another thread during polling messages
   */
  public void contains(final Map<Integer, List<M>> expected, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(collectionStream,
        "This util is intended to use only on CollectionStream)");
    Map<Integer, List<M>> actual = TestRunner.consumeStream(collectionStream, timeout);
    for (Integer paritionId: expected.keySet()) {
      assertThat(actual.get(paritionId), IsIterableContainingInOrder.contains(expected.get(paritionId).toArray()));
    }
  }
}
