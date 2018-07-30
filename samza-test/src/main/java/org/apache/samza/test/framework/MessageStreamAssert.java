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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.io.ObjectInputStream;
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

import static org.junit.Assert.assertThat;

/**
 * An assertion on the content of a {@link MessageStream}.
 *
 * <pre>Example: {@code
 * MessageStream<String> stream = streamGraph.getInputStream("input", serde).map(some_function)...;
 * ...
 * MessageStreamAssert.that(id, stream, stringSerde).containsInAnyOrder(Arrays.asList("a", "b", "c"));
 * }</pre>
 *
 */
@VisibleForTesting
public class MessageStreamAssert<M> {
  private final static Map<String, CountDownLatch> LATCHES = new ConcurrentHashMap<>();
  private final static CountDownLatch PLACE_HOLDER = new CountDownLatch(0);

  private final String id;
  private final MessageStream<M> messageStream;
  private final Serde<M> serde;
  private boolean checkEachTask = false;

  /**
   * Constructors a MessageStreamAssert with an id and serde
   * @param id unique id
   * @param messageStream represents messageStream that you want to assert on
   * @param serde serde used to desialize messageStream
   * @param <M> represents type of Message
   * @return MessageStreamAssert that returns the the messages in the stream
   */
  public static <M> MessageStreamAssert<M> that(String id, MessageStream<M> messageStream, Serde<M> serde) {
    return new MessageStreamAssert<>(id, messageStream, serde);
  }

  private MessageStreamAssert(String id, MessageStream<M> messageStream, Serde<M> serde) {
    this.id = id;
    this.messageStream = messageStream;
    this.serde = serde;
  }

  public MessageStreamAssert forEachTask() {
    checkEachTask = true;
    return this;
  }

  public void containsInAnyOrder(final Collection<M> expected) {
    LATCHES.putIfAbsent(id, PLACE_HOLDER);
    final MessageStream<M> streamToCheck = checkEachTask
        ? messageStream
        : messageStream
            .partitionBy(m -> null, m -> m, KVSerde.of(new StringSerde(), serde), null)
            .map(kv -> kv.value);

    streamToCheck.sink(new CheckAgainstExpected<M>(id, expected, checkEachTask));
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

  private static final class CheckAgainstExpected<M> implements SinkFunction<M> {
    private static final long TIMEOUT = 5000L;

    private final String id;
    private final boolean checkEachTask;
    private final Collection<M> expected;


    private transient Timer timer = new Timer();
    private transient List<M> actual = Collections.synchronizedList(new ArrayList<>());
    private transient TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        check();
      }
    };

    CheckAgainstExpected(String id, Collection<M> expected, boolean checkEachTask) {
      this.id = id;
      this.expected = expected;
      this.checkEachTask = checkEachTask;
    }

    @Override
    public void init(Config config, TaskContext context) {
      final SystemStreamPartition ssp = Iterables.getFirst(context.getSystemStreamPartitions(), null);
      if (ssp != null || ssp.getPartition().getPartitionId() == 0) {
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
        assertThat(actual, Matchers.containsInAnyOrder((M[]) expected.toArray()));
        throw new IllegalArgumentException("asdas");
      } finally {
        latch.countDown();
      }
    }
  }
}
