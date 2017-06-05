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

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.SystemClock;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestOperatorImplGraph {

  @Test
  public void testOperatorGraphInitAndClose() {
    ApplicationRunner mockRunner = mock(ApplicationRunner.class);
    StreamSpec testStreamSpec1 = new StreamSpec("test-stream-1", "physical-stream-1", "test-system");
    when(mockRunner.getStreamSpec("test-stream-1")).thenReturn(testStreamSpec1);
    StreamSpec testStreamSpec2 = new StreamSpec("test-stream-2", "physical-stream-2", "test-system");
    when(mockRunner.getStreamSpec("test-stream-2")).thenReturn(testStreamSpec2);

    Config mockConfig = mock(Config.class);
    TaskContext mockContext = createMockContext();
    StreamGraphImpl graph = new StreamGraphImpl(mockRunner, mockConfig);

    List<String> initializationOrder = new ArrayList<>();
    List<String> finalizationOrder = new ArrayList<>();

    MessageStream<Object> inputStream1 = graph.getInputStream("test-stream-1", (k, v) -> v);
    MessageStream<Object> inputStream2 = graph.getInputStream("test-stream-2", (k, v) -> v);

    inputStream1.map(createMapFunction("1", initializationOrder, finalizationOrder))
               .map(createMapFunction("2", initializationOrder, finalizationOrder));

    inputStream2.map(createMapFunction("3", initializationOrder, finalizationOrder))
        .map(createMapFunction("4", initializationOrder, finalizationOrder));

    OperatorImplGraph implGraph = new OperatorImplGraph(SystemClock.instance());

    // Assert that initialization occurs in topological order.
    implGraph.init(graph, mockConfig, mockContext);
    assertEquals(initializationOrder.get(0), "1");
    assertEquals(initializationOrder.get(1), "2");
    assertEquals(initializationOrder.get(2), "3");
    assertEquals(initializationOrder.get(3), "4");

    // Assert that finalization occurs in reverse topological order.
    implGraph.close();
    assertEquals(finalizationOrder.get(0), "4");
    assertEquals(finalizationOrder.get(1), "3");
    assertEquals(finalizationOrder.get(2), "2");
    assertEquals(finalizationOrder.get(3), "1");
  }

  private TaskContext createMockContext() {
    TaskContext mockContext = mock(TaskContext.class);
    when(mockContext.getMetricsRegistry()).thenReturn(new MetricsRegistryMap());
    return mockContext;
  }

  /**
   * Creates an identity map function that appends to the provided lists when init/close is invoked.
   */
  private MapFunction<Object, Object> createMapFunction(String id, List<String> initializationOrder, List<String> finalizationOrder) {
    return new MapFunction<Object, Object>() {
      @Override
      public void init(Config config, TaskContext context) {
        initializationOrder.add(id);
      }

      @Override
      public void close() {
        finalizationOrder.add(id);
      }

      @Override
      public Object apply(Object message) {
        return message;
      }
    };
  }
}

