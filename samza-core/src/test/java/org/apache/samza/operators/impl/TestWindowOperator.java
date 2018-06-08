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

package org.apache.samza.operators.impl;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.TaskContextImpl;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraphSpec;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.impl.store.TestInMemoryStore;
import org.apache.samza.operators.impl.store.TimeSeriesKeySerde;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.triggers.FiringType;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.testUtils.TestClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestWindowOperator {
  private final TaskCoordinator taskCoordinator = mock(TaskCoordinator.class);
  private final List<Integer> integers = ImmutableList.of(1, 2, 1, 2, 1, 2, 1, 2, 3);
  private Config config;
  private TaskContextImpl taskContext;
  private ApplicationRunner runner;

  @Before
  public void setup() throws Exception {
    config = mock(Config.class);
    when(config.get(JobConfig.JOB_NAME())).thenReturn("jobName");
    when(config.get(eq(JobConfig.JOB_ID()), anyString())).thenReturn("jobId");
    taskContext = mock(TaskContextImpl.class);
    runner = mock(ApplicationRunner.class);
    Serde storeKeySerde = new TimeSeriesKeySerde(new IntegerSerde());
    Serde storeValSerde = KVSerde.of(new IntegerSerde(), new IntegerSerde());

    when(taskContext.getSystemStreamPartitions()).thenReturn(ImmutableSet
        .of(new SystemStreamPartition("kafka", "integers", new Partition(0))));
    when(taskContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    when(taskContext.getStore("jobName-jobId-window-w1"))
        .thenReturn(new TestInMemoryStore<>(storeKeySerde, storeValSerde));
    when(runner.getStreamSpec("integers")).thenReturn(new StreamSpec("integers", "integers", "kafka"));

    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put("app.runner.class", "org.apache.samza.runtime.LocalApplicationRunner");
    mapConfig.put("job.default.system", "kafka");
    mapConfig.put("job.name", "jobName");
    mapConfig.put("job.id", "jobId");
    config = new MapConfig(mapConfig);
  }

  @Test
  public void testTumblingWindowsDiscardingMode() throws Exception {

    OperatorSpecGraph sgb = this.getKeyedTumblingWindowStreamGraph(AccumulationMode.DISCARDING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2))).getOperatorSpecGraph();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();

    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);
    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    integers.forEach(n -> task.process(new IntegerEnvelope(n), messageCollector, taskCoordinator));
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
  public void testNonKeyedTumblingWindowsDiscardingMode() throws Exception {

    OperatorSpecGraph sgb = this.getTumblingWindowStreamGraph(AccumulationMode.DISCARDING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(1000))).getOperatorSpecGraph();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();

    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);

    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    Assert.assertEquals(windowPanes.size(), 0);

    integers.forEach(n -> task.process(new IntegerEnvelope(n), messageCollector, taskCoordinator));
    Assert.assertEquals(windowPanes.size(), 0);

    testClock.advanceTime(Duration.ofSeconds(1));
    Assert.assertEquals(windowPanes.size(), 0);

    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 1);
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 9);
  }

  @Test
  public void testTumblingAggregatingWindowsDiscardingMode() throws Exception {

    when(taskContext.getStore("jobName-jobId-window-w1"))
        .thenReturn(new TestInMemoryStore<>(new TimeSeriesKeySerde(new IntegerSerde()), new IntegerSerde()));

    OperatorSpecGraph sgb = this.getAggregateTumblingWindowStreamGraph(AccumulationMode.DISCARDING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2))).getOperatorSpecGraph();
    List<WindowPane<Integer, Integer>> windowPanes = new ArrayList<>();

    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);
    MessageCollector messageCollector = envelope -> windowPanes.add((WindowPane<Integer, Integer>) envelope.getMessage());
    integers.forEach(n -> task.process(new IntegerEnvelope(n), messageCollector, taskCoordinator));
    testClock.advanceTime(Duration.ofSeconds(1));

    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 5);
    Assert.assertEquals(windowPanes.get(0).getMessage(), new Integer(2));
    Assert.assertEquals(windowPanes.get(1).getMessage(), new Integer(2));
    Assert.assertEquals(windowPanes.get(2).getMessage(), new Integer(2));
    Assert.assertEquals(windowPanes.get(3).getMessage(), new Integer(2));
    Assert.assertEquals(windowPanes.get(4).getMessage(), new Integer(1));
  }

  @Test
  public void testTumblingWindowsAccumulatingMode() throws Exception {
    OperatorSpecGraph sgb = this.getKeyedTumblingWindowStreamGraph(AccumulationMode.ACCUMULATING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2))).getOperatorSpecGraph();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);

    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    integers.forEach(n -> task.process(new IntegerEnvelope(n), messageCollector, taskCoordinator));
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
    OperatorSpecGraph sgb =
        this.getKeyedSessionWindowStreamGraph(AccumulationMode.DISCARDING, Duration.ofMillis(500)).getOperatorSpecGraph();
    TestClock testClock = new TestClock();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);
    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 1);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "1");
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));

    task.process(new IntegerEnvelope(2), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(2), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(3), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(3), messageCollector, taskCoordinator);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 3);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "1");
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "1001");
    Assert.assertEquals(windowPanes.get(2).getKey().getPaneId(), "1001");
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 2);
    Assert.assertEquals((windowPanes.get(1).getMessage()).size(), 2);
    Assert.assertEquals((windowPanes.get(2).getMessage()).size(), 2);

    task.process(new IntegerEnvelope(2), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(2), messageCollector, taskCoordinator);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 4);
    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), new Integer(2));
    Assert.assertEquals(windowPanes.get(3).getKey().getPaneId(), "2001");
    Assert.assertEquals((windowPanes.get(3).getMessage()).size(), 2);
  }

  @Test
  public void testSessionWindowsAccumulatingMode() throws Exception {
    OperatorSpecGraph sgb = this.getKeyedSessionWindowStreamGraph(AccumulationMode.DISCARDING,
        Duration.ofMillis(500)).getOperatorSpecGraph();
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();

    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    task.init(config, taskContext);

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    testClock.advanceTime(Duration.ofSeconds(1));

    task.process(new IntegerEnvelope(2), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(2), messageCollector, taskCoordinator);

    task.process(new IntegerEnvelope(2), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(2), messageCollector, taskCoordinator);

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
    OperatorSpecGraph sgb = this.getKeyedTumblingWindowStreamGraph(AccumulationMode.ACCUMULATING,
        Duration.ofSeconds(1), Triggers.count(2)).getOperatorSpecGraph();
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);

    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 1);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "0");
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(0).getFiringType(), FiringType.EARLY);

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 1);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 2);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "0");
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "0");
    Assert.assertEquals(windowPanes.get(1).getFiringType(), FiringType.DEFAULT);

    task.process(new IntegerEnvelope(3), messageCollector, taskCoordinator);
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
    OperatorSpecGraph sgb = this.getKeyedTumblingWindowStreamGraph(AccumulationMode.ACCUMULATING, Duration.ofSeconds(1),
        Triggers.any(Triggers.count(2), Triggers.timeSinceFirstMessage(Duration.ofMillis(500)))).getOperatorSpecGraph();
    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);

    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    //assert that the count trigger fired
    Assert.assertEquals(windowPanes.size(), 1);

    //advance the timer to enable the triggering of the inner timeSinceFirstMessage trigger
    testClock.advanceTime(Duration.ofMillis(500));

    //assert that the triggering of the count trigger cancelled the inner timeSinceFirstMessage trigger
    Assert.assertEquals(windowPanes.size(), 1);

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);

    //advance timer by 500 more millis to enable the default trigger
    testClock.advanceTime(Duration.ofMillis(500));
    task.window(messageCollector, taskCoordinator);

    //assert that the default trigger fired
    Assert.assertEquals(windowPanes.size(), 2);
    Assert.assertEquals(windowPanes.get(1).getFiringType(), FiringType.DEFAULT);
    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "0");
    Assert.assertEquals((windowPanes.get(1).getMessage()).size(), 5);

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);

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

    OperatorSpecGraph sgb = this.getKeyedTumblingWindowStreamGraph(AccumulationMode.ACCUMULATING, Duration.ofSeconds(1),
        Triggers.repeat(Triggers.any(Triggers.count(2), Triggers.timeSinceFirstMessage(Duration.ofMillis(500))))).getOperatorSpecGraph();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();

    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());

    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    //assert that the count trigger fired
    Assert.assertEquals(windowPanes.size(), 1);

    //advance the timer to enable the potential triggering of the inner timeSinceFirstMessage trigger
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    testClock.advanceTime(Duration.ofMillis(500));
    //assert that the triggering of the count trigger cancelled the inner timeSinceFirstMessage trigger
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 2);

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 3);

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    //advance timer by 500 more millis to enable the default trigger
    testClock.advanceTime(Duration.ofMillis(500));
    task.window(messageCollector, taskCoordinator);
    //assert that the default trigger fired
    Assert.assertEquals(windowPanes.size(), 4);
  }

  @Test
  public void testEndOfStreamFlushesWithEarlyTriggerFirings() throws Exception {
    EndOfStreamStates endOfStreamStates = new EndOfStreamStates(ImmutableSet.of(new SystemStreamPartition("kafka",
        "integers", new Partition(0))), Collections.emptyMap());

    when(taskContext.getTaskName()).thenReturn(new TaskName("task 1"));
    when(taskContext.fetchObject(EndOfStreamStates.class.getName())).thenReturn(endOfStreamStates);
    when(taskContext.fetchObject(WatermarkStates.class.getName())).thenReturn(mock(WatermarkStates.class));

    OperatorSpecGraph sgb = this.getTumblingWindowStreamGraph(AccumulationMode.DISCARDING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2))).getOperatorSpecGraph();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();

    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);

    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    Assert.assertEquals(windowPanes.size(), 0);

    List<Integer> integerList = ImmutableList.of(1, 2, 1, 2, 1);
    integerList.forEach(n -> task.process(new IntegerEnvelope(n), messageCollector, taskCoordinator));

    // early triggers should emit (1,2) and (1,2) in the same window.
    Assert.assertEquals(windowPanes.size(), 2);

    testClock.advanceTime(Duration.ofSeconds(1));
    Assert.assertEquals(windowPanes.size(), 2);

    final IncomingMessageEnvelope endOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(
        new SystemStreamPartition("kafka", "integers", new Partition(0)));
    task.process(endOfStream, messageCollector, taskCoordinator);

    // end of stream flushes the last entry (1)
    Assert.assertEquals(windowPanes.size(), 3);
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 2);
    verify(taskCoordinator, times(1)).commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    verify(taskCoordinator, times(1)).shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
  }

  @Test
  public void testEndOfStreamFlushesWithDefaultTriggerFirings() throws Exception {
    EndOfStreamStates endOfStreamStates = new EndOfStreamStates(ImmutableSet.of(new SystemStreamPartition("kafka",
        "integers", new Partition(0))), Collections.emptyMap());

    when(taskContext.getTaskName()).thenReturn(new TaskName("task 1"));
    when(taskContext.fetchObject(EndOfStreamStates.class.getName())).thenReturn(endOfStreamStates);
    when(taskContext.fetchObject(WatermarkStates.class.getName())).thenReturn(mock(WatermarkStates.class));

    OperatorSpecGraph sgb =
        this.getKeyedSessionWindowStreamGraph(AccumulationMode.DISCARDING, Duration.ofMillis(500)).getOperatorSpecGraph();
    TestClock testClock = new TestClock();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);

    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    testClock.advanceTime(1000);
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 1);

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);

    final IncomingMessageEnvelope endOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(
        new SystemStreamPartition("kafka", "integers", new Partition(0)));
    task.process(endOfStream, messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 2);
    Assert.assertEquals(windowPanes.get(0).getMessage().size(), 2);
    verify(taskCoordinator, times(1)).commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    verify(taskCoordinator, times(1)).shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
  }

  @Test
  public void testEndOfStreamFlushesWithNoTriggerFirings() throws Exception {
    EndOfStreamStates endOfStreamStates = new EndOfStreamStates(ImmutableSet.of(new SystemStreamPartition("kafka",
        "integers", new Partition(0))), Collections.emptyMap());

    when(taskContext.getTaskName()).thenReturn(new TaskName("task 1"));
    when(taskContext.fetchObject(EndOfStreamStates.class.getName())).thenReturn(endOfStreamStates);
    when(taskContext.fetchObject(WatermarkStates.class.getName())).thenReturn(mock(WatermarkStates.class));

    OperatorSpecGraph sgb =
        this.getKeyedSessionWindowStreamGraph(AccumulationMode.DISCARDING, Duration.ofMillis(500)).getOperatorSpecGraph();
    TestClock testClock = new TestClock();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    StreamOperatorTask task = new StreamOperatorTask(sgb, null, testClock);
    task.init(config, taskContext);

    MessageCollector messageCollector =
        envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());

    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);
    task.process(new IntegerEnvelope(1), messageCollector, taskCoordinator);

    final IncomingMessageEnvelope endOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(
        new SystemStreamPartition("kafka", "integers", new Partition(0)));
    task.process(endOfStream, messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 1);
    Assert.assertEquals(windowPanes.get(0).getMessage().size(), 4);
    verify(taskCoordinator, times(1)).commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    verify(taskCoordinator, times(1)).shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
  }

  private StreamGraphSpec getKeyedTumblingWindowStreamGraph(AccumulationMode mode,
      Duration duration, Trigger<KV<Integer, Integer>> earlyTrigger) throws IOException {
    StreamGraphSpec graph = new StreamGraphSpec(runner, config);

    KVSerde<Integer, Integer> kvSerde = KVSerde.of(new IntegerSerde(), new IntegerSerde());
    graph.getInputStream("integers", kvSerde)
        .window(Windows.keyedTumblingWindow(KV::getKey, duration, new IntegerSerde(), kvSerde)
            .setEarlyTrigger(earlyTrigger).setAccumulationMode(mode), "w1")
        .sink((message, messageCollector, taskCoordinator) -> {
            SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
            messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
          });

    return graph;
  }

  private StreamGraphSpec getTumblingWindowStreamGraph(AccumulationMode mode,
      Duration duration, Trigger<KV<Integer, Integer>> earlyTrigger) throws IOException {
    StreamGraphSpec graph = new StreamGraphSpec(runner, config);

    KVSerde<Integer, Integer> kvSerde = KVSerde.of(new IntegerSerde(), new IntegerSerde());
    graph.getInputStream("integers", kvSerde)
        .window(Windows.tumblingWindow(duration, kvSerde).setEarlyTrigger(earlyTrigger)
            .setAccumulationMode(mode), "w1")
        .sink((message, messageCollector, taskCoordinator) -> {
            SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
            messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
          });
    return graph;
  }

  private StreamGraphSpec getKeyedSessionWindowStreamGraph(AccumulationMode mode, Duration duration) throws IOException {
    StreamGraphSpec graph = new StreamGraphSpec(runner, config);

    KVSerde<Integer, Integer> kvSerde = KVSerde.of(new IntegerSerde(), new IntegerSerde());
    graph.getInputStream("integers", kvSerde)
        .window(Windows.keyedSessionWindow(KV::getKey, duration, new IntegerSerde(), kvSerde)
            .setAccumulationMode(mode), "w1")
        .sink((message, messageCollector, taskCoordinator) -> {
            SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
            messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
          });
    return graph;
  }

  private StreamGraphSpec getAggregateTumblingWindowStreamGraph(AccumulationMode mode, Duration timeDuration,
        Trigger<IntegerEnvelope> earlyTrigger) throws IOException {
    StreamGraphSpec graph = new StreamGraphSpec(runner, config);

    MessageStream<KV<Integer, Integer>> integers = graph.getInputStream("integers",
        KVSerde.of(new IntegerSerde(), new IntegerSerde()));

    integers
        .map(new KVMapFunction())
        .window(Windows.<IntegerEnvelope, Integer>tumblingWindow(timeDuration, () -> 0, (m, c) -> c + 1, new IntegerSerde())
            .setEarlyTrigger(earlyTrigger)
            .setAccumulationMode(mode), "w1")
        .sink((message, messageCollector, taskCoordinator) -> {
            SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
            messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
          });
    return graph;
  }

  private static class IntegerEnvelope extends IncomingMessageEnvelope {

    IntegerEnvelope(Integer key) {
      super(new SystemStreamPartition("kafka", "integers", new Partition(0)), null, key, key);
    }
  }

  private static class KVMapFunction implements MapFunction<KV<Integer, Integer>, IntegerEnvelope> {

    @Override
    public IntegerEnvelope apply(KV<Integer, Integer> message) {
      return new IntegerEnvelope(message.getKey());
    }
  }

}
