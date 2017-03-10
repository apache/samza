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

package org.apache.samza.operators.triggers;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphBuilder;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;
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
  private static final Clock CLOCK = SystemClock.instance();

  @Before
  public void setup() throws Exception {
    windowPanes.clear();

    config = mock(Config.class);
    taskContext = mock(TaskContext.class);
    when(taskContext.getSystemStreamPartitions()).thenReturn(ImmutableSet
        .of(new SystemStreamPartition("kafka", "integers", new Partition(0))));

  }

  @Test
  public void testTumblingWindowsDiscardingMode() throws Exception {

    StreamGraphBuilder sgb = new KeyedTumblingWindowStreamGraphBuilder(AccumulationMode.DISCARDING, Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2)));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(config, taskContext);

    integers.forEach(n -> task.process(new IntegerMessageEnvelope(n, n), messageCollector, taskCoordinator));
    testClock.advanceTime(Duration.ofSeconds(1));

    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 5);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));
    Assert.assertEquals(((Collection) windowPanes.get(0).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), new Integer(2));
    Assert.assertEquals(((Collection) windowPanes.get(1).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(2).getKey().getKey(), new Integer(1));
    Assert.assertEquals(((Collection) windowPanes.get(2).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), new Integer(2));
    Assert.assertEquals(((Collection) windowPanes.get(3).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(4).getKey().getKey(), new Integer(3));
    Assert.assertEquals(((Collection) windowPanes.get(4).getMessage()).size(), 1);
  }

  @Test
  public void testTumblingWindowsAccumulatingMode() throws Exception {
    StreamGraphBuilder sgb = new KeyedTumblingWindowStreamGraphBuilder(AccumulationMode.ACCUMULATING, Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2)));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(config, taskContext);

    integers.forEach(n -> task.process(new IntegerMessageEnvelope(n, n), messageCollector, taskCoordinator));
    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 7);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));
    Assert.assertEquals(((Collection) windowPanes.get(0).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), new Integer(2));
    Assert.assertEquals(((Collection) windowPanes.get(1).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(2).getKey().getKey(), new Integer(1));
    Assert.assertEquals(((Collection) windowPanes.get(2).getMessage()).size(), 4);

    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), new Integer(2));
    Assert.assertEquals(((Collection) windowPanes.get(3).getMessage()).size(), 4);
  }

  @Test
  public void testSessionWindowsDiscardingMode() throws Exception {
    StreamGraphBuilder sgb = new KeyedSessionWindowStreamGraphBuilder(AccumulationMode.DISCARDING, Duration.ofMillis(500));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 1);

    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);

    task.process(new IntegerMessageEnvelope(3, 3), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(3, 3), messageCollector, taskCoordinator);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 3);
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "1001");

    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 4);
    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), new Integer(2));
    Assert.assertEquals(windowPanes.get(3).getKey().getPaneId(), "2001");

    Assert.assertEquals(((Collection) windowPanes.get(2).getMessage()).size(), 2);
  }

  @Test
  public void testSessionWindowsAccumulatingMode() throws Exception {
    StreamGraphBuilder sgb = new KeyedSessionWindowStreamGraphBuilder(AccumulationMode.DISCARDING, Duration.ofMillis(500));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
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
    Assert.assertEquals(((Collection) windowPanes.get(0).getMessage()).size(), 2);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), new Integer(2));
    Assert.assertEquals(((Collection) windowPanes.get(0).getMessage()).size(), 2);
    Assert.assertEquals(((Collection) windowPanes.get(1).getMessage()).size(), 4);
  }

  @Test
  public void testCancelationOfOnceTrigger() throws Exception {
    StreamGraphBuilder sgb = new KeyedTumblingWindowStreamGraphBuilder(AccumulationMode.ACCUMULATING, Duration.ofSeconds(1), Triggers.count(2));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 3), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 4), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 5), messageCollector, taskCoordinator);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 2);
  }

  @Test
  public void testCancelationOfAnyTrigger() throws Exception {
    StreamGraphBuilder sgb = new KeyedTumblingWindowStreamGraphBuilder(AccumulationMode.ACCUMULATING, Duration.ofSeconds(1),
        Triggers.any(Triggers.count(2), Triggers.timeSinceFirstMessage(Duration.ofMillis(500))));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
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
  }

  @Test
  public void testCancelationOfRepeatingNestedTriggers() throws Exception {

    StreamGraphBuilder sgb = new KeyedTumblingWindowStreamGraphBuilder(AccumulationMode.ACCUMULATING, Duration.ofSeconds(1),
        Triggers.repeat(Triggers.any(Triggers.count(2), Triggers.timeSinceFirstMessage(Duration.ofMillis(500)))));
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    System.out.println("process 12");

    task.process(new IntegerMessageEnvelope(1, 2), messageCollector, taskCoordinator);
    //assert that the count trigger fired
    Assert.assertEquals(windowPanes.size(), 1);
    //advance the timer to enable the triggering of the inner timeSinceFirstMessage trigger
    System.out.println("process 13");
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

  private class KeyedTumblingWindowStreamGraphBuilder implements StreamGraphBuilder {

    private final StreamSpec streamSpec = new StreamSpec("integer-stream", "integers", "kafka");
    private final AccumulationMode mode;
    private final Duration duration;
    private final Trigger<MessageEnvelope<Integer, Integer>> earlyTrigger;

    KeyedTumblingWindowStreamGraphBuilder(AccumulationMode mode, Duration timeDuration, Trigger<MessageEnvelope<Integer, Integer>> earlyTrigger) {
      this.mode = mode;
      this.duration = timeDuration;
      this.earlyTrigger = earlyTrigger;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<MessageEnvelope<Integer, Integer>> inStream = graph.createInStream(streamSpec, null, null);
      Function<MessageEnvelope<Integer, Integer>, Integer> keyFn = m -> m.getKey();
      inStream
        .map(m -> (m))
        .window(Windows.keyedTumblingWindow(keyFn, duration).setEarlyTrigger(earlyTrigger)
          .setAccumulationMode(mode))
        .map(m -> {
            windowPanes.add(m);
            return m;
          });
    }
  }

  private class KeyedSessionWindowStreamGraphBuilder implements StreamGraphBuilder {

    private final StreamSpec streamSpec = new StreamSpec("integer-stream", "integers", "kafka");
    private final AccumulationMode mode;
    private final Duration duration;

    KeyedSessionWindowStreamGraphBuilder(AccumulationMode mode, Duration duration) {
      this.mode = mode;
      this.duration = duration;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<MessageEnvelope<Integer, Integer>> inStream = graph.createInStream(streamSpec, null, null);
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
}
