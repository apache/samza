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
import org.apache.samza.operators.windows.WindowKey;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
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
  private final List<Integer> mapOutput = new ArrayList<>();
  private final List<WindowPane> windowPanes = new ArrayList<>();
  private final List<Integer> integers = ImmutableList.of(1, 2, 1, 2, 1, 2, 1, 2, 3);
  private Config config;
  private TaskContext taskContext;

  @Before
  public void setup() throws Exception {
    mapOutput.clear();
    windowPanes.clear();

    config = mock(Config.class);
    taskContext = mock(TaskContext.class);
    when(taskContext.getSystemStreamPartitions()).thenReturn(ImmutableSet
        .of(new SystemStreamPartition("kafka", "integers", new Partition(0))));

  }

  @Test
  public void testTumblingWindowsDiscardingMode() throws Exception {
    StreamGraphBuilder sgb = new KeyedTumblingWindowStreamGraphBuilder(AccumulationMode.DISCARDING);
    StreamOperatorTask task = new StreamOperatorTask(sgb);
    task.init(config, taskContext);

    integers.forEach(n -> task.process(new IntegerMessageEnvelope(n, n), messageCollector, taskCoordinator));
    Thread.sleep(1000);
    task.window(messageCollector, taskCoordinator);

    Assert.assertEquals(windowPanes.size(), 5);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), 1);
    Assert.assertEquals(((Collection) windowPanes.get(0).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), 2);
    Assert.assertEquals(((Collection) windowPanes.get(1).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(2).getKey().getKey(), 1);
    Assert.assertEquals(((Collection) windowPanes.get(2).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), 2);
    Assert.assertEquals(((Collection) windowPanes.get(3).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(4).getKey().getKey(), 3);
    Assert.assertEquals(((Collection) windowPanes.get(4).getMessage()).size(), 1);
  }

  @Test
  public void testTumblingWindowsAccumulatingMode() throws Exception {
    StreamGraphBuilder sgb = new KeyedTumblingWindowStreamGraphBuilder(AccumulationMode.ACCUMULATING);
    StreamOperatorTask task = new StreamOperatorTask(sgb);
    task.init(config, taskContext);
    System.out.println("PRINT ME1");

    integers.forEach(n -> task.process(new IntegerMessageEnvelope(n, n), messageCollector, taskCoordinator));
    Thread.sleep(1000);
    task.window(messageCollector, taskCoordinator);
    System.out.println("PRINT ME2");

    Assert.assertEquals(windowPanes.size(), 7);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), 1);
    System.out.println("PRINT ME3");
    System.out.println("Size of " + ((Collection) windowPanes.get(0).getMessage()).size());
    Assert.assertEquals(((Collection) windowPanes.get(0).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), 2);
    Assert.assertEquals(((Collection) windowPanes.get(1).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(2).getKey().getKey(), 1);
    Assert.assertEquals(((Collection) windowPanes.get(2).getMessage()).size(), 4);

    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), 2);
    Assert.assertEquals(((Collection) windowPanes.get(3).getMessage()).size(), 4);
  }

  @Test
  public void testSessionWindowsDiscardingMode() throws Exception {
    StreamGraphBuilder sgb = new KeyedSessionWindowStreamGraphBuilder(AccumulationMode.DISCARDING);
    StreamOperatorTask task = new StreamOperatorTask(sgb);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);

    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);

    task.process(new IntegerMessageEnvelope(3, 3), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(3, 3), messageCollector, taskCoordinator);

    Thread.sleep(1000);
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 3);
    Assert.assertEquals(((Collection) windowPanes.get(2).getMessage()).size(), 2);
  }

  @Test
  public void testSessionWindowsAccumulatingMode() throws Exception {
    StreamGraphBuilder sgb = new KeyedSessionWindowStreamGraphBuilder(AccumulationMode.DISCARDING);
    StreamOperatorTask task = new StreamOperatorTask(sgb);
    task.init(config, taskContext);

    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(1, 1), messageCollector, taskCoordinator);
    Thread.sleep(1000);

    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);

    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);
    task.process(new IntegerMessageEnvelope(2, 2), messageCollector, taskCoordinator);

    Thread.sleep(1000);
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 2);
    Assert.assertEquals(((Collection) windowPanes.get(0).getMessage()).size(), 2);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), 1);
    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), 2);
    Assert.assertEquals(((Collection) windowPanes.get(0).getMessage()).size(), 2);
    Assert.assertEquals(((Collection) windowPanes.get(1).getMessage()).size(), 4);
  }


    private class KeyedTumblingWindowStreamGraphBuilder implements StreamGraphBuilder {

    private final StreamSpec streamSpec = new StreamSpec("integer-stream", "integers", "kafka");
    private final AccumulationMode mode;

    KeyedTumblingWindowStreamGraphBuilder(AccumulationMode mode) {
      this.mode = mode;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<MessageEnvelope<Integer, Integer>> inStream = graph.createInStream(streamSpec, null, null);
      Function<MessageEnvelope<Integer, Integer>, Integer> keyFn = m -> m.getKey();

      inStream
        .map(m -> {
            mapOutput.add(m.getKey());
            return m;
          })
        .window(Windows.keyedTumblingWindow(keyFn, Duration.ofSeconds(1)).setEarlyTrigger(Triggers.repeat(Triggers.count(2)))
          .setAccumulationMode(mode))
        .map(m -> {
          System.out.println("inside window panes " + m.getKey() + " " + ((Collection)m.getMessage()).size());
            windowPanes.add(m);
          printWindowPanes();
            WindowKey<Integer> key = m.getKey();
            Collection<MessageEnvelope<Integer, Integer>> message = m.getMessage();
            ArrayList<MessageEnvelope<Integer, Integer>> list = new ArrayList<MessageEnvelope<Integer, Integer>>(message);
            return m;
          });
    }
  }

  private class KeyedSessionWindowStreamGraphBuilder implements StreamGraphBuilder {

    private final StreamSpec streamSpec = new StreamSpec("integer-stream", "integers", "kafka");
    private final AccumulationMode mode;

    KeyedSessionWindowStreamGraphBuilder(AccumulationMode mode) {
      this.mode = mode;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<MessageEnvelope<Integer, Integer>> inStream = graph.createInStream(streamSpec, null, null);
      Function<MessageEnvelope<Integer, Integer>, Integer> keyFn = m -> m.getKey();

      inStream
          .map(m -> {
            mapOutput.add(m.getKey());
            return m;
          })
          .window(Windows.keyedSessionWindow(keyFn, Duration.ofSeconds(1))
              .setAccumulationMode(mode))
          .map(m -> {
            System.out.println("inside window panes " + m.getKey() + " " + ((Collection)m.getMessage()).size());
            windowPanes.add(m);
            printWindowPanes();
            WindowKey<Integer> key = m.getKey();
            Collection<MessageEnvelope<Integer, Integer>> message = m.getMessage();
            ArrayList<MessageEnvelope<Integer, Integer>> list = new ArrayList<MessageEnvelope<Integer, Integer>>(message);
            return m;
          });
    }
  }



  private void printWindowPanes() {
    System.out.println("=====");
    for(WindowPane pane : windowPanes) {
      System.out.println(pane.getKey() + "  " + ((ArrayList)pane.getMessage()).size() );
    }
    System.out.println("=====");
  }
  private class IntegerMessageEnvelope extends IncomingMessageEnvelope {
    IntegerMessageEnvelope(int key, int msg) {
      super(new SystemStreamPartition("kafka", "integers", new Partition(0)), "1", key, msg);
    }
  }
}
