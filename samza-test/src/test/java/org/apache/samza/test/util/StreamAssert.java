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

package org.apache.samza.test.util;

import com.google.common.collect.Iterables;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertThat;

/**
 * An assertion on the content of a {@link MessageStream}.
 *
 * <p>Example: </pre>{@code
 * MessageStream<String> stream = streamGraph.getInputStream("input", serde).map(some_function)...;
 * ...
 * StreamAssert.that(stream, stringSerde).containsInAnyOrder(Arrays.asList("a", "b", "c"));
 * }</pre>
 *
 */
public class StreamAssert<M> {
  private final static Map<Integer, CountDownLatch> latches = new ConcurrentHashMap<>();

  private final MessageStream<M> messageStream;
  private final Serde<M> serde;
  private boolean checkEachTask = false;

  public static <M> StreamAssert<M> that(MessageStream<M> messageStream, Serde<M> serde) {
    return new StreamAssert<>(messageStream, serde);
  }

  private StreamAssert(MessageStream<M> messageStream, Serde<M> serde) {
    this.messageStream = messageStream;
    this.serde = serde;
  }

  public StreamAssert forEachTask() {
    checkEachTask = true;
    return this;
  }

  public void containsInAnyOrder(final Collection<M> expected) {
    final MessageStream<M> streamToCheck = checkEachTask
        ? messageStream
        : messageStream
          .partitionBy(m -> null, m -> m, KVSerde.of(new StringSerde(), serde), null)
          .map(kv -> kv.value);

    streamToCheck.sink(new CheckAgainstExpected<M>(expected, checkEachTask));
  }

  public static void waitForComplete() {
    try {
      while (latches.isEmpty()) {
        Thread.sleep(100);
      }

      while(!latches.isEmpty()) {
        for (Iterator<CountDownLatch> iter = latches.values().iterator(); iter.hasNext(); ) {
          iter.next().await();
          iter.remove();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final class CheckAgainstExpected<M> implements SinkFunction<M> {
    private static int ID = 0;
    private static final long timeout = 5000L;

    private final boolean checkEachTask;
    private final Collection<M> expected;

    private transient int id;
    private transient Timer timer = new Timer();
    private transient List<M> actual = Collections.synchronizedList(new ArrayList<>());
    private transient AtomicBoolean done = new AtomicBoolean(false);
    private transient TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        check();
      }
    };

    CheckAgainstExpected(Collection<M> expected, boolean checkEachTask) {
      this.expected = expected;
      this.checkEachTask = checkEachTask;
      this.id = ID++;
    }

    @Override
    public void init(Config config, TaskContext context) {
      final SystemStreamPartition ssp = Iterables.getFirst(context.getSystemStreamPartitions(), null);
      final boolean check = checkEachTask
          || (ssp == null ? false : ssp.getPartition().getPartitionId() == 0);
      if (check) {
        latches.put(id, new CountDownLatch(1));
        timer.schedule(timerTask, timeout);
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

    private void check() {
      final CountDownLatch latch = latches.get(id);
      try {
        assertThat(actual, Matchers.containsInAnyOrder((M[]) expected.toArray()));
      } finally {
        if (done.compareAndSet(false, true)) {
          latch.countDown();
        }
      }
    }
  }
}
