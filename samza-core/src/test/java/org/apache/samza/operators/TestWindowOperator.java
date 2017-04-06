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

package org.apache.samza.operators;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.triggers.FiringType;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.testUtils.TestClock;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestWindowOperator {
  private final MessageCollector messageCollector = mock(MessageCollector.class);
  private final TaskCoordinator taskCoordinator = mock(TaskCoordinator.class);
  private final List<WindowPane<Integer, Collection<MessageEnvelope<Integer, Integer>>>> windowPanes = new ArrayList<>();
  private final List<Integer> integers = ImmutableList.of(1, 2, 1, 2, 1, 2, 1, 2, 3);
  private Config config;
  private TaskContext taskContext;
  private ApplicationRunner runner;

  @Before
  public void setup() throws Exception {
    windowPanes.clear();

    config = mock(Config.class);
    taskContext = mock(TaskContext.class);
    runner = mock(ApplicationRunner.class);
    when(taskContext.getSystemStreamPartitions()).thenReturn(ImmutableSet
        .of(new SystemStreamPartition("kafka", "integers", new Partition(0))));
    when(runner.getStreamSpec("integer-stream")).thenReturn(new StreamSpec("integer-stream", "integers", "kafka"));
  }

  @Test
  public void testTumblingWindowsDiscardingMode() throws Exception {

    StreamApplication sgb = new KeyedTumblingWindowStreamApplication(AccumulationMode.DISCARDING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2)));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, runner, testClock);
    task.init(config, taskContext);

    integers.forEach(n -> task.process(new IntegerMessageEnvelope(n, n), messageCollector, taskCoordinator));
    testClock.advanceTime(Duration.ofSeconds(1));

    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 5);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), new Integer(2));
    Assert.assertEquals((windowPanes.get(1).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(2).getKey().getKey(), new Integer(1));
    Assert.assertEquals((windowPanes.get(2).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), new Integer(2));
    Assert.assertEquals((windowPanes.get(3).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(4).getKey().getKey(), new Integer(3));
    Assert.assertEquals((windowPanes.get(4).getMessage()).size(), 1);
  }

  @Test
  public void testTumblingWindowsAccumulatingMode() throws Exception {
    StreamApplication sgb = new KeyedTumblingWindowStreamApplication(AccumulationMode.ACCUMULATING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2)));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, runner, testClock);
    task.init(config, taskContext);

    integers.forEach(n -> task.process(new IntegerMessageEnvelope(n, n), messageCollector, taskCoordinator));
    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 7);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), new Integer(2));
    Assert.assertEquals((windowPanes.get(1).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(2).getKey().getKey(), new Integer(1));
    Assert.assertEquals((windowPanes.get(2).getMessage()).size(), 4);

    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), new Integer(2));
    Assert.assertEquals((windowPanes.get(3).getMessage()).size(), 4);
  }

  @Test
  public void testSessionWindowsDiscardingMode() throws Exception {
    StreamApplication sgb = new KeyedSessionWindowStreamApplication(AccumulationMode.DISCARDING, Duration.ofMillis(500));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, runner, testClock);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 1);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "1");
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));

    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(3, 3), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(3, 3), messageCollector, taskCoordinator);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 3);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "1");
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "1001");
    Assert.assertEquals(windowPanes.get(2).getKey().getPaneId(), "1001");
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 2);
    Assert.assertEquals((windowPanes.get(1).getMessage()).size(), 2);
    Assert.assertEquals((windowPanes.get(2).getMessage()).size(), 2);

    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 4);
    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), new Integer(2));
    Assert.assertEquals(windowPanes.get(3).getKey().getPaneId(), "2001");
    Assert.assertEquals((windowPanes.get(3).getMessage()).size(), 2);

  }

  @Test
  public void testSessionWindowsAccumulatingMode() throws Exception {
    StreamApplication sgb = new KeyedSessionWindowStreamApplication(AccumulationMode.DISCARDING,
        Duration.ofMillis(500));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, runner, testClock);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    testClock.advanceTime(Duration.ofSeconds(1));

    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);

    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 2);
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 2);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), new Integer(2));
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 2);
    Assert.assertEquals((windowPanes.get(1).getMessage()).size(), 4);
  }

  @Test
  public void testCancellationOfOnceTrigger() throws Exception {
    StreamApplication sgb = new KeyedTumblingWindowStreamApplication(AccumulationMode.ACCUMULATING,
        Duration.ofSeconds(1), Triggers.count(2));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, runner, testClock);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 2), messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 1);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "0");
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(0).getFiringType(), FiringType.EARLY);

    task.process(new IntegerMessageEnvelope(1, 3), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 4), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 5), messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 1);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 2);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "0");
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "0");
    Assert.assertEquals(windowPanes.get(1).getFiringType(), FiringType.DEFAULT);

    task.process(new IntegerMessageEnvelope(3, 6), messageCollector, taskCoordinator);
    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 3);
    Assert.assertEquals(windowPanes.get(2).getKey().getKey(), new Integer(3));
    Assert.assertEquals(windowPanes.get(2).getKey().getPaneId(), "1000");
    Assert.assertEquals(windowPanes.get(2).getFiringType(), FiringType.DEFAULT);
    Assert.assertEquals((windowPanes.get(2).getMessage()).size(), 1);

  }

  @Test
  public void testCancellationOfAnyTrigger() throws Exception {
    StreamApplication sgb = new KeyedTumblingWindowStreamApplication(AccumulationMode.ACCUMULATING, Duration.ofSeconds(1),
        Triggers.any(Triggers.count(2), Triggers.timeSinceFirstMessage(Duration.ofMillis(500))));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, runner, testClock);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 2), messageCollector, taskCoordinator);
    //assert that the count trigger fired
    Assert.assertEquals(windowPanes.size(), 1);

    //advance the timer to enable the triggering of the inner timeSinceFirstMessage trigger
    testClock.advanceTime(Duration.ofMillis(500));

    //assert that the triggering of the count trigger cancelled the inner timeSinceFirstMessage trigger
    Assert.assertEquals(windowPanes.size(), 1);

    task.process(new IntegerMessageEnvelope(1, 3), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 4), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 5), messageCollector, taskCoordinator);

    //advance timer by 500 more millis to enable the default trigger
    testClock.advanceTime(Duration.ofMillis(500));
    task.window(messageCollector, taskCoordinator);

    //assert that the default trigger fired
    Assert.assertEquals(windowPanes.size(), 2);
    Assert.assertEquals(windowPanes.get(1).getFiringType(), FiringType.DEFAULT);
    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "0");
    Assert.assertEquals((windowPanes.get(1).getMessage()).size(), 5);

    task.process(new IntegerMessageEnvelope(1, 5), messageCollector, taskCoordinator);

    //advance timer by 500 millis to enable the inner timeSinceFirstMessage trigger
    testClock.advanceTime(Duration.ofMillis(500));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 3);
    Assert.assertEquals(windowPanes.get(2).getFiringType(), FiringType.EARLY);
    Assert.assertEquals(windowPanes.get(2).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(2).getKey().getPaneId(), "1000");

    //advance timer by > 500 millis to enable the default trigger
    testClock.advanceTime(Duration.ofMillis(900));
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 4);
    Assert.assertEquals(windowPanes.get(3).getFiringType(), FiringType.DEFAULT);
    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(3).getKey().getPaneId(), "1000");
  }

  @Test
  public void testCancelationOfRepeatingNestedTriggers() throws Exception {

    StreamApplication sgb = new KeyedTumblingWindowStreamApplication(AccumulationMode.ACCUMULATING, Duration.ofSeconds(1),
        Triggers.repeat(Triggers.any(Triggers.count(2), Triggers.timeSinceFirstMessage(Duration.ofMillis(500)))));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, runner, testClock);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);

    task.process(new IntegerMessageEnvelope(1, 2), messageCollector, taskCoordinator);
    //assert that the count trigger fired
    Assert.assertEquals(windowPanes.size(), 1);

    //advance the timer to enable the potential triggering of the inner timeSinceFirstMessage trigger
    task.process(new IntegerMessageEnvelope(1, 3), messageCollector, taskCoordinator);
    testClock.advanceTime(Duration.ofMillis(500));
    //assert that the triggering of the count trigger cancelled the inner timeSinceFirstMessage trigger
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 2);

    task.process(new IntegerMessageEnvelope(1, 3), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 4), messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 3);

    task.process(new IntegerMessageEnvelope(1, 5), messageCollector, taskCoordinator);
    //advance timer by 500 more millis to enable the default trigger
    testClock.advanceTime(Duration.ofMillis(500));
    task.window(messageCollector, taskCoordinator);
    //assert that the default trigger fired
    Assert.assertEquals(windowPanes.size(), 4);
  }

  private class KeyedTumblingWindowStreamApplication implements StreamApplication {

    private final AccumulationMode mode;
    private final Duration duration;
    private final Trigger<MessageEnvelope<Integer, Integer>> earlyTrigger;

    KeyedTumblingWindowStreamApplication(AccumulationMode mode,
        Duration timeDuration, Trigger<MessageEnvelope<Integer, Integer>> earlyTrigger) {
      this.mode = mode;
      this.duration = timeDuration;
      this.earlyTrigger = earlyTrigger;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<MessageEnvelope<Integer, Integer>> inStream = graph.getInputStream("integer-stream",
          (k, m) -> new MessageEnvelope(k, m));
      Function<MessageEnvelope<Integer, Integer>, Integer> keyFn = m -> m.getKey();
      inStream
        .map(m -> m)
        .window(Windows.keyedTumblingWindow(keyFn, duration).setEarlyTrigger(earlyTrigger)
          .setAccumulationMode(mode))
        .map(m -> {
            windowPanes.add(m);
            return m;
          });
    }
  }

  private class KeyedSessionWindowStreamApplication implements StreamApplication {

    private final AccumulationMode mode;
    private final Duration duration;

    KeyedSessionWindowStreamApplication(AccumulationMode mode, Duration duration) {
      this.mode = mode;
      this.duration = duration;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<MessageEnvelope<Integer, Integer>> inStream = graph.getInputStream("integer-stream",
          (k, m) -> new MessageEnvelope(k, m));
      Function<MessageEnvelope<Integer, Integer>, Integer> keyFn = m -> m.getKey();

      inStream
          .map(m -> m)
          .window(Windows.keyedSessionWindow(keyFn, duration)
              .setAccumulationMode(mode))
          .map(m -> {
              windowPanes.add(m);
              return m;
            });
    }
  }

  private class IntegerMessageEnvelope extends IncomingMessageEnvelope {
    IntegerMessageEnvelope(int key, int msg) {
      super(new SystemStreamPartition("kafka", "integers", new Partition(0)), "1", key, msg);
    }
  }

  private class MessageEnvelope<K, V> {
    private final K key;
    private final V value;

    MessageEnvelope(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }
  }
}
