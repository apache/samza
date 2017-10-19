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

package org.apache.samza.control;

//import java.time.Duration;
//import java.util.Arrays;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.function.BiFunction;
//import java.util.function.Function;
//import org.apache.samza.config.Config;
//import org.apache.samza.config.JobConfig;
//import org.apache.samza.config.MapConfig;
//import org.apache.samza.control.IOGraph.IONode;
//import org.apache.samza.operators.MessageStream;
//import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraphImpl;
//import org.apache.samza.operators.functions.JoinFunction;
//import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
//import org.junit.Before;
//import org.junit.Test;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;


public class TestIOGraph {
  StreamSpec input1;
  StreamSpec input2;
  StreamSpec input3;
  StreamSpec output1;
  StreamSpec output2;
  StreamSpec int1;
  StreamSpec int2;

  StreamGraphImpl streamGraph;

//  @Before
//  public void setup() {
//    ApplicationRunner runner = mock(ApplicationRunner.class);
//    Map<String, String> configMap = new HashMap<>();
//    configMap.put(JobConfig.JOB_NAME(), "test-app");
//    configMap.put(JobConfig.JOB_DEFAULT_SYSTEM(), "test-system");
//    Config config = new MapConfig(configMap);
//
//    /**
//     * the graph looks like the following. number of partitions in parentheses. quotes indicate expected value.
//     *
//     *                                    input1 -> map -> join -> output1
//     *                                                       |
//     *                      input2 -> partitionBy -> filter -|
//     *                                                       |
//     *           input3 -> filter -> partitionBy -> map -> join -> output2
//     *
//     */
//    input1 = new StreamSpec("input1", "input1", "system1");
//    input2 = new StreamSpec("input2", "input2", "system2");
//    input3 = new StreamSpec("input3", "input3", "system2");
//
//    output1 = new StreamSpec("output1", "output1", "system1");
//    output2 = new StreamSpec("output2", "output2", "system2");
//
//    runner = mock(ApplicationRunner.class);
//    when(runner.getStreamSpec("input1")).thenReturn(input1);
//    when(runner.getStreamSpec("input2")).thenReturn(input2);
//    when(runner.getStreamSpec("input3")).thenReturn(input3);
//    when(runner.getStreamSpec("output1")).thenReturn(output1);
//    when(runner.getStreamSpec("output2")).thenReturn(output2);
//
//    // intermediate streams used in tests
//    int1 = new StreamSpec("test-app-1-partition_by-3", "test-app-1-partition_by-3", "default-system");
//    int2 = new StreamSpec("test-app-1-partition_by-8", "test-app-1-partition_by-8", "default-system");
//    when(runner.getStreamSpec("test-app-1-partition_by-3"))
//        .thenReturn(int1);
//    when(runner.getStreamSpec("test-app-1-partition_by-8"))
//        .thenReturn(int2);
//
//    streamGraph = new StreamGraphImpl(runner, config);
//    BiFunction msgBuilder = mock(BiFunction.class);
//    MessageStream m1 = streamGraph.getInputStream("input1", msgBuilder).map(m -> m);
//    MessageStream m2 = streamGraph.getInputStream("input2", msgBuilder).partitionBy(m -> "haha").filter(m -> true);
//    MessageStream m3 = streamGraph.getInputStream("input3", msgBuilder).filter(m -> true).partitionBy(m -> "hehe").map(m -> m);
//    Function mockFn = mock(Function.class);
//    OutputStream<Object, Object, Object> om1 = streamGraph.getOutputStream("output1", mockFn, mockFn);
//    OutputStream<Object, Object, Object> om2 = streamGraph.getOutputStream("output2", mockFn, mockFn);
//
//    m1.join(m2, mock(JoinFunction.class), Duration.ofHours(2)).sendTo(om1);
//    m3.join(m2, mock(JoinFunction.class), Duration.ofHours(1)).sendTo(om2);
//  }
//
//  @Test
//  public void testBuildIOGraph() {
//    IOGraph ioGraph = streamGraph.toIOGraph();
//    assertEquals(ioGraph.getNodes().size(), 4);
//
//    for (IONode node : ioGraph.getNodes()) {
//      if (node.getOutput().equals(output1)) {
//        assertEquals(node.getInputs().size(), 2);
//        assertFalse(node.isOutputIntermediate());
//        StreamSpec[] inputs = sort(node.getInputs());
//        assertEquals(inputs[0], input1);
//        assertEquals(inputs[1], int1);
//      } else if (node.getOutput().equals(output2)) {
//        assertEquals(node.getInputs().size(), 2);
//        assertFalse(node.isOutputIntermediate());
//        StreamSpec[] inputs = sort(node.getInputs());
//        assertEquals(inputs[0], int1);
//        assertEquals(inputs[1], int2);
//      } else if (node.getOutput().equals(int1)) {
//        assertEquals(node.getInputs().size(), 1);
//        assertTrue(node.isOutputIntermediate());
//        StreamSpec[] inputs = sort(node.getInputs());
//        assertEquals(inputs[0], input2);
//      } else if (node.getOutput().equals(int2)) {
//        assertEquals(node.getInputs().size(), 1);
//        assertTrue(node.isOutputIntermediate());
//        StreamSpec[] inputs = sort(node.getInputs());
//        assertEquals(inputs[0], input3);
//      }
//    }
//  }
//
//  @Test
//  public void testNodesOfInput() {
//    IOGraph ioGraph = streamGraph.toIOGraph();
//    Collection<IONode> nodes = ioGraph.getNodesOfInput(input1.toSystemStream());
//    assertEquals(nodes.size(), 1);
//    IONode node = nodes.iterator().next();
//    assertEquals(node.getOutput(), output1);
//    assertEquals(node.getInputs().size(), 2);
//    assertFalse(node.isOutputIntermediate());
//
//    nodes = ioGraph.getNodesOfInput(input2.toSystemStream());
//    assertEquals(nodes.size(), 1);
//    node = nodes.iterator().next();
//    assertEquals(node.getOutput(), int1);
//    assertEquals(node.getInputs().size(), 1);
//    assertTrue(node.isOutputIntermediate());
//
//    nodes = ioGraph.getNodesOfInput(int1.toSystemStream());
//    assertEquals(nodes.size(), 2);
//    nodes.forEach(n -> {
//        assertEquals(n.getInputs().size(), 2);
//      });
//
//    nodes = ioGraph.getNodesOfInput(input3.toSystemStream());
//    assertEquals(nodes.size(), 1);
//    node = nodes.iterator().next();
//    assertEquals(node.getOutput(), int2);
//    assertEquals(node.getInputs().size(), 1);
//    assertTrue(node.isOutputIntermediate());
//
//    nodes = ioGraph.getNodesOfInput(int2.toSystemStream());
//    assertEquals(nodes.size(), 1);
//    node = nodes.iterator().next();
//    assertEquals(node.getOutput(), output2);
//    assertEquals(node.getInputs().size(), 2);
//    assertFalse(node.isOutputIntermediate());
//  }
//
//  private static StreamSpec[] sort(Set<StreamSpec> specs) {
//    StreamSpec[] array = new StreamSpec[specs.size()];
//    specs.toArray(array);
//    Arrays.sort(array, (s1, s2) -> s1.getId().compareTo(s2.getId()));
//    return array;
//  }
//
//  public static IOGraph buildSimpleIOGraph(List<StreamSpec> inputs,
//      StreamSpec output,
//      boolean isOutputIntermediate) {
//    IONode node = new IONode(output, isOutputIntermediate);
//    inputs.forEach(input -> node.addInput(input));
//    return new IOGraph(Collections.singleton(node));
//  }
}
