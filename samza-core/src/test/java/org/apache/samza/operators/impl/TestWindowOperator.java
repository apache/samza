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
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.samza.Partition;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptorImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.Context;
import org.apache.samza.context.MockContext;
import org.apache.samza.context.TaskContextImpl;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.impl.store.TestInMemoryStore;
import org.apache.samza.operators.impl.store.TimeSeriesKeySerde;
import org.apache.samza.operators.triggers.FiringType;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.testUtils.TestClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.samza.testUtils.StreamTestUtils.*;
import static org.mockito.Mockito.*;


public class TestWindowOperator {
  private final TaskCoordinator taskCoordinator = mock(TaskCoordinator.class);
  private final TaskCallback taskCallback = mock(TaskCallback.class);
  private final List<Integer> integers = ImmutableList.of(1, 2, 1, 2, 1, 2, 1, 2, 3);
  private Context context;
  private Config config;

  @Before
  public void setup() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("job.default.system", "kafka");
    configMap.put("job.name", "jobName");
    configMap.put("job.id", "jobId");
    this.config = new MapConfig(configMap);

    this.context = new MockContext();
    when(this.context.getJobContext().getConfig()).thenReturn(this.config);
    Serde storeKeySerde = new TimeSeriesKeySerde(new IntegerSerde());
    Serde storeValSerde = KVSerde.of(new IntegerSerde(), new IntegerSerde());

    SystemStreamPartition ssp = new SystemStreamPartition("kafka", "integers", new Partition(0));
    TaskModel taskModel = mock(TaskModel.class);
    when(taskModel.getSystemStreamPartitions()).thenReturn(ImmutableSet.of(ssp));
    when(taskModel.getTaskName()).thenReturn(new TaskName("task 1"));
    when(this.context.getTaskContext().getTaskModel()).thenReturn(taskModel);
    when(((TaskContextImpl) this.context.getTaskContext()).getSspsExcludingSideInputs()).thenReturn(
        ImmutableSet.of(ssp));
    when(this.context.getTaskContext().getTaskMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    when(this.context.getTaskContext().getOperatorExecutor()).thenReturn(Executors.newSingleThreadExecutor());
    when(this.context.getContainerContext().getContainerMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    when(this.context.getTaskContext().getStore("jobName-jobId-window-w1"))
        .thenReturn(new TestInMemoryStore<>(storeKeySerde, storeValSerde));
  }

  @Test
  public void testTumblingWindowsDiscardingMode() throws Exception {
    OperatorSpecGraph sgb = this.getKeyedTumblingWindowStreamGraph(AccumulationMode.DISCARDING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2))).getOperatorSpecGraph();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();

    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);
    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    integers.forEach(n -> processSync(task,  new IntegerEnvelope(n), messageCollector, taskCoordinator, taskCallback));
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
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);

    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    Assert.assertEquals(windowPanes.size(), 0);

    integers.forEach(n -> processSync(task,  new IntegerEnvelope(n), messageCollector, taskCoordinator, taskCallback));
    Assert.assertEquals(windowPanes.size(), 0);

    testClock.advanceTime(Duration.ofSeconds(1));
    Assert.assertEquals(windowPanes.size(), 0);

    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 1);
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 9);
  }

  @Test
  public void testTumblingAggregatingWindowsDiscardingMode() throws Exception {
    when(this.context.getTaskContext().getStore("jobName-jobId-window-w1"))
        .thenReturn(new TestInMemoryStore<>(new TimeSeriesKeySerde(new IntegerSerde()), new IntegerSerde()));

    OperatorSpecGraph sgb = this.getAggregateTumblingWindowStreamGraph(AccumulationMode.DISCARDING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2))).getOperatorSpecGraph();
    List<WindowPane<Integer, Integer>> windowPanes = new ArrayList<>();

    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);
    MessageCollector messageCollector = envelope -> windowPanes.add((WindowPane<Integer, Integer>) envelope.getMessage());
    integers.forEach(n -> processSync(task,  new IntegerEnvelope(n), messageCollector, taskCoordinator, taskCallback));
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
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);

    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    integers.forEach(n -> processSync(task,  new IntegerEnvelope(n), messageCollector, taskCoordinator, taskCallback));
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
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);
    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    processSync(task,  new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task,  new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 1);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "1");
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));

    processSync(task,  new IntegerEnvelope(2), messageCollector, taskCoordinator, taskCallback);
    processSync(task,  new IntegerEnvelope(2), messageCollector, taskCoordinator, taskCallback);
    processSync(task,  new IntegerEnvelope(3), messageCollector, taskCoordinator, taskCallback);
    processSync(task,  new IntegerEnvelope(3), messageCollector, taskCoordinator, taskCallback);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 3);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "1");
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "1001");
    Assert.assertEquals(windowPanes.get(2).getKey().getPaneId(), "1001");
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 2);
    Assert.assertEquals((windowPanes.get(1).getMessage()).size(), 2);
    Assert.assertEquals((windowPanes.get(2).getMessage()).size(), 2);

    processSync(task, new IntegerEnvelope(2), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(2), messageCollector, taskCoordinator, taskCallback);

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
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();

    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    task.init(this.context);

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    testClock.advanceTime(Duration.ofSeconds(1));

    processSync(task, new IntegerEnvelope(2), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(2), messageCollector, taskCoordinator, taskCallback);

    processSync(task, new IntegerEnvelope(2), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(2), messageCollector, taskCoordinator, taskCallback);

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
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);

    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    Assert.assertEquals(windowPanes.size(), 1);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "0");
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(0).getFiringType(), FiringType.EARLY);

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);

    Assert.assertEquals(windowPanes.size(), 1);

    testClock.advanceTime(Duration.ofSeconds(1));
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 2);
    Assert.assertEquals(windowPanes.get(0).getKey().getPaneId(), "0");
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "0");
    Assert.assertEquals(windowPanes.get(1).getFiringType(), FiringType.DEFAULT);

    processSync(task, new IntegerEnvelope(3), messageCollector, taskCoordinator, taskCallback);
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
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);

    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    //assert that the count trigger fired
    Assert.assertEquals(windowPanes.size(), 1);

    //advance the timer to enable the triggering of the inner timeSinceFirstMessage trigger
    testClock.advanceTime(Duration.ofMillis(500));

    //assert that the triggering of the count trigger cancelled the inner timeSinceFirstMessage trigger
    Assert.assertEquals(windowPanes.size(), 1);

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);

    //advance timer by 500 more millis to enable the default trigger
    testClock.advanceTime(Duration.ofMillis(500));
    task.window(messageCollector, taskCoordinator);

    //assert that the default trigger fired
    Assert.assertEquals(windowPanes.size(), 2);
    Assert.assertEquals(windowPanes.get(1).getFiringType(), FiringType.DEFAULT);
    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), new Integer(1));
    Assert.assertEquals(windowPanes.get(1).getKey().getPaneId(), "0");
    Assert.assertEquals((windowPanes.get(1).getMessage()).size(), 5);

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);

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
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    //assert that the count trigger fired
    Assert.assertEquals(windowPanes.size(), 1);

    //advance the timer to enable the potential triggering of the inner timeSinceFirstMessage trigger
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    testClock.advanceTime(Duration.ofMillis(500));
    //assert that the triggering of the count trigger cancelled the inner timeSinceFirstMessage trigger
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 2);

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    Assert.assertEquals(windowPanes.size(), 3);

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    //advance timer by 500 more millis to enable the default trigger
    testClock.advanceTime(Duration.ofMillis(500));
    task.window(messageCollector, taskCoordinator);
    //assert that the default trigger fired
    Assert.assertEquals(windowPanes.size(), 4);
  }

  @Test
  public void testEndOfStreamFlushesWithEarlyTriggerFirings() throws Exception {

    OperatorSpecGraph sgb = this.getTumblingWindowStreamGraph(AccumulationMode.DISCARDING,
        Duration.ofSeconds(1), Triggers.repeat(Triggers.count(2))).getOperatorSpecGraph();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();

    TestClock testClock = new TestClock();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);

    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());
    Assert.assertEquals(windowPanes.size(), 0);

    List<Integer> integerList = ImmutableList.of(1, 2, 1, 2, 1);
    integerList.forEach(n -> processSync(task, new IntegerEnvelope(n), messageCollector, taskCoordinator, taskCallback));

    // early triggers should emit (1,2) and (1,2) in the same window.
    Assert.assertEquals(windowPanes.size(), 2);

    testClock.advanceTime(Duration.ofSeconds(1));
    Assert.assertEquals(windowPanes.size(), 2);

    final IncomingMessageEnvelope endOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(
        new SystemStreamPartition("kafka", "integers", new Partition(0)));
    processSync(task, endOfStream, messageCollector, taskCoordinator, taskCallback);

    // end of stream flushes the last entry (1)
    Assert.assertEquals(windowPanes.size(), 3);
    Assert.assertEquals((windowPanes.get(0).getMessage()).size(), 2);
    verify(taskCoordinator, times(1)).commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    verify(taskCoordinator, times(1)).shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
  }

  @Test
  public void testEndOfStreamFlushesWithDefaultTriggerFirings() throws Exception {
    OperatorSpecGraph sgb =
        this.getKeyedSessionWindowStreamGraph(AccumulationMode.DISCARDING, Duration.ofMillis(500)).getOperatorSpecGraph();
    TestClock testClock = new TestClock();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);

    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    testClock.advanceTime(1000);
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 1);

    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task, new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);

    final IncomingMessageEnvelope endOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(
        new SystemStreamPartition("kafka", "integers", new Partition(0)));
    processSync(task, endOfStream, messageCollector, taskCoordinator, taskCallback);
    Assert.assertEquals(2, windowPanes.size());
    Assert.assertEquals(2, windowPanes.get(0).getMessage().size());
    verify(taskCoordinator, times(1)).commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    verify(taskCoordinator, times(1)).shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
  }

  @Test
  public void testEndOfStreamFlushesWithNoTriggerFirings() throws Exception {
    OperatorSpecGraph sgb =
        this.getKeyedSessionWindowStreamGraph(AccumulationMode.DISCARDING, Duration.ofMillis(500)).getOperatorSpecGraph();
    TestClock testClock = new TestClock();
    List<WindowPane<Integer, Collection<IntegerEnvelope>>> windowPanes = new ArrayList<>();
    StreamOperatorTask task = new StreamOperatorTask(sgb, testClock);
    task.init(this.context);


    MessageCollector messageCollector =
      envelope -> windowPanes.add((WindowPane<Integer, Collection<IntegerEnvelope>>) envelope.getMessage());

    processSync(task,  new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task,  new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task,  new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);
    processSync(task,  new IntegerEnvelope(1), messageCollector, taskCoordinator, taskCallback);

    final IncomingMessageEnvelope endOfStream = IncomingMessageEnvelope.buildEndOfStreamEnvelope(
        new SystemStreamPartition("kafka", "integers", new Partition(0)));
    processSync(task,  endOfStream, messageCollector, taskCoordinator, taskCallback);
    Assert.assertEquals(1, windowPanes.size());
    Assert.assertEquals(4, windowPanes.get(0).getMessage().size());
    verify(taskCoordinator, times(1)).commit(TaskCoordinator.RequestScope.CURRENT_TASK);
    verify(taskCoordinator, times(1)).shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
  }

  private StreamApplicationDescriptorImpl getKeyedTumblingWindowStreamGraph(AccumulationMode mode,
      Duration duration, Trigger<KV<Integer, Integer>> earlyTrigger) throws IOException {

    StreamApplication userApp = appDesc -> {
      KVSerde<Integer, Integer> kvSerde = KVSerde.of(new IntegerSerde(), new IntegerSerde());
      GenericSystemDescriptor sd = new GenericSystemDescriptor("kafka", "mockFactoryClass");
      GenericInputDescriptor<KV<Integer, Integer>> inputDescriptor = sd.getInputDescriptor("integers", kvSerde);
      appDesc.getInputStream(inputDescriptor)
          .window(Windows.keyedTumblingWindow(KV::getKey, duration, new IntegerSerde(), kvSerde)
              .setEarlyTrigger(earlyTrigger).setAccumulationMode(mode), "w1")
          .sink((message, messageCollector, taskCoordinator) -> {
            SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
            messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
          });
    };

    return new StreamApplicationDescriptorImpl(userApp, config);
  }

  private StreamApplicationDescriptorImpl getTumblingWindowStreamGraph(AccumulationMode mode,
      Duration duration, Trigger<KV<Integer, Integer>> earlyTrigger) throws IOException {
    StreamApplication userApp = appDesc -> {
      KVSerde<Integer, Integer> kvSerde = KVSerde.of(new IntegerSerde(), new IntegerSerde());
      GenericSystemDescriptor sd = new GenericSystemDescriptor("kafka", "mockFactoryClass");
      GenericInputDescriptor<KV<Integer, Integer>> inputDescriptor = sd.getInputDescriptor("integers", kvSerde);
      appDesc.getInputStream(inputDescriptor)
          .window(Windows.tumblingWindow(duration, kvSerde).setEarlyTrigger(earlyTrigger)
              .setAccumulationMode(mode), "w1")
          .sink((message, messageCollector, taskCoordinator) -> {
            SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
            messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
          });
    };

    return new StreamApplicationDescriptorImpl(userApp, config);
  }

  private StreamApplicationDescriptorImpl getKeyedSessionWindowStreamGraph(AccumulationMode mode, Duration duration) throws IOException {
    StreamApplication userApp = appDesc -> {
      KVSerde<Integer, Integer> kvSerde = KVSerde.of(new IntegerSerde(), new IntegerSerde());
      GenericSystemDescriptor sd = new GenericSystemDescriptor("kafka", "mockFactoryClass");
      GenericInputDescriptor<KV<Integer, Integer>> inputDescriptor = sd.getInputDescriptor("integers", kvSerde);
      appDesc.getInputStream(inputDescriptor)
          .window(Windows.keyedSessionWindow(KV::getKey, duration, new IntegerSerde(), kvSerde)
              .setAccumulationMode(mode), "w1")
          .sink((message, messageCollector, taskCoordinator) -> {
            SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
            messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
          });
    };

    return new StreamApplicationDescriptorImpl(userApp, config);
  }

  private StreamApplicationDescriptorImpl getAggregateTumblingWindowStreamGraph(AccumulationMode mode, Duration timeDuration,
        Trigger<IntegerEnvelope> earlyTrigger) throws IOException {
    StreamApplication userApp = appDesc -> {
      KVSerde<Integer, Integer> kvSerde = KVSerde.of(new IntegerSerde(), new IntegerSerde());
      GenericSystemDescriptor sd = new GenericSystemDescriptor("kafka", "mockFactoryClass");
      GenericInputDescriptor<KV<Integer, Integer>> inputDescriptor = sd.getInputDescriptor("integers", kvSerde);
      MessageStream<KV<Integer, Integer>> integers = appDesc.getInputStream(inputDescriptor);

      integers
          .map(new KVMapFunction())
          .window(Windows.<IntegerEnvelope, Integer>tumblingWindow(timeDuration, () -> 0, (m, c) -> c + 1, new IntegerSerde())
              .setEarlyTrigger(earlyTrigger)
              .setAccumulationMode(mode), "w1")
          .sink((message, messageCollector, taskCoordinator) -> {
            SystemStream outputSystemStream = new SystemStream("outputSystem", "outputStream");
            messageCollector.send(new OutgoingMessageEnvelope(outputSystemStream, message));
          });
    };

    return new StreamApplicationDescriptorImpl(userApp, config);
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
