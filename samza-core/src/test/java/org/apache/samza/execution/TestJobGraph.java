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

package org.apache.samza.execution;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.operators.OperatorSpecGraph;
import org.apache.samza.system.StreamSpec;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestJobGraph {

  JobGraph graph1;
  JobGraph graph2;
  JobGraph graph3;
  JobGraph graph4;
  int streamSeq = 0;

  private StreamSpec genStream() {
    ++streamSeq;

    return new StreamSpec(String.valueOf(streamSeq), "test-stream", "test-system");
  }

  /**
   * graph1 is the example graph from wikipedia
   *
   * 5   7   3
   * | / | / |
   * v   v   |
   * 11  8   |
   * | \X   /
   * v v \v
   * 2 9 10
   */
  private void createGraph1() {
    OperatorSpecGraph specGraph = mock(OperatorSpecGraph.class);
    when(specGraph.getBroadcastStreams()).thenReturn(Collections.emptySet());
    graph1 = new JobGraph(null, specGraph);

    JobNode n2 = graph1.getOrCreateJobNode("2", "1");
    JobNode n3 = graph1.getOrCreateJobNode("3", "1");
    JobNode n5 = graph1.getOrCreateJobNode("5", "1");
    JobNode n7 = graph1.getOrCreateJobNode("7", "1");
    JobNode n8 = graph1.getOrCreateJobNode("8", "1");
    JobNode n9 = graph1.getOrCreateJobNode("9", "1");
    JobNode n10 = graph1.getOrCreateJobNode("10", "1");
    JobNode n11 = graph1.getOrCreateJobNode("11", "1");

    graph1.addSource(genStream(), n5);
    graph1.addSource(genStream(), n7);
    graph1.addSource(genStream(), n3);
    graph1.addIntermediateStream(genStream(), n5, n11);
    graph1.addIntermediateStream(genStream(), n7, n11);
    graph1.addIntermediateStream(genStream(), n7, n8);
    graph1.addIntermediateStream(genStream(), n3, n8);
    graph1.addIntermediateStream(genStream(), n11, n2);
    graph1.addIntermediateStream(genStream(), n11, n9);
    graph1.addIntermediateStream(genStream(), n8, n9);
    graph1.addIntermediateStream(genStream(), n11, n10);
    graph1.addSink(genStream(), n2);
    graph1.addSink(genStream(), n9);
    graph1.addSink(genStream(), n10);
  }

  /**
   * graph2 is a graph with a loop
   * 1 -> 2 -> 3 -> 4 -> 5 -> 7
   *      |<---6 <--|    <>
   */
  private void createGraph2() {
    OperatorSpecGraph specGraph = mock(OperatorSpecGraph.class);
    when(specGraph.getBroadcastStreams()).thenReturn(Collections.emptySet());
    graph2 = new JobGraph(null, specGraph);

    JobNode n1 = graph2.getOrCreateJobNode("1", "1");
    JobNode n2 = graph2.getOrCreateJobNode("2", "1");
    JobNode n3 = graph2.getOrCreateJobNode("3", "1");
    JobNode n4 = graph2.getOrCreateJobNode("4", "1");
    JobNode n5 = graph2.getOrCreateJobNode("5", "1");
    JobNode n6 = graph2.getOrCreateJobNode("6", "1");
    JobNode n7 = graph2.getOrCreateJobNode("7", "1");

    graph2.addSource(genStream(), n1);
    graph2.addIntermediateStream(genStream(), n1, n2);
    graph2.addIntermediateStream(genStream(), n2, n3);
    graph2.addIntermediateStream(genStream(), n3, n4);
    graph2.addIntermediateStream(genStream(), n4, n5);
    graph2.addIntermediateStream(genStream(), n4, n6);
    graph2.addIntermediateStream(genStream(), n6, n2);
    graph2.addIntermediateStream(genStream(), n5, n5);
    graph2.addIntermediateStream(genStream(), n5, n7);
    graph2.addSink(genStream(), n7);
  }

  /**
   * graph3 is a graph with two self loops
   * 1<->1 -> 2<->2
   */
  private void createGraph3() {
    OperatorSpecGraph specGraph = mock(OperatorSpecGraph.class);
    when(specGraph.getBroadcastStreams()).thenReturn(Collections.emptySet());
    graph3 = new JobGraph(null, specGraph);

    JobNode n1 = graph3.getOrCreateJobNode("1", "1");
    JobNode n2 = graph3.getOrCreateJobNode("2", "1");

    graph3.addSource(genStream(), n1);
    graph3.addIntermediateStream(genStream(), n1, n1);
    graph3.addIntermediateStream(genStream(), n1, n2);
    graph3.addIntermediateStream(genStream(), n2, n2);
  }

  /**
   * graph4 is a graph of single-loop node
   * 1<->1
   */
  private void createGraph4() {
    OperatorSpecGraph specGraph = mock(OperatorSpecGraph.class);
    when(specGraph.getBroadcastStreams()).thenReturn(Collections.emptySet());
    graph4 = new JobGraph(null, specGraph);

    JobNode n1 = graph4.getOrCreateJobNode("1", "1");

    graph4.addSource(genStream(), n1);
    graph4.addIntermediateStream(genStream(), n1, n1);
  }

  @Before
  public void setup() {
    createGraph1();
    createGraph2();
    createGraph3();
    createGraph4();
  }

  @Test
  public void testAddSource() {
    OperatorSpecGraph specGraph = mock(OperatorSpecGraph.class);
    when(specGraph.getBroadcastStreams()).thenReturn(Collections.emptySet());
    JobGraph graph = new JobGraph(null, specGraph);

    /**
     * s1 -> 1
     * s2 ->|
     *
     * s3 -> 2
     *   |-> 3
     */
    JobNode n1 = graph.getOrCreateJobNode("1", "1");
    JobNode n2 = graph.getOrCreateJobNode("2", "1");
    JobNode n3 = graph.getOrCreateJobNode("3", "1");
    StreamSpec s1 = genStream();
    StreamSpec s2 = genStream();
    StreamSpec s3 = genStream();
    graph.addSource(s1, n1);
    graph.addSource(s2, n1);
    graph.addSource(s3, n2);
    graph.addSource(s3, n3);

    assertTrue(graph.getSources().size() == 3);

    assertTrue(graph.getOrCreateJobNode("1", "1").getInEdges().size() == 2);
    assertTrue(graph.getOrCreateJobNode("2", "1").getInEdges().size() == 1);
    assertTrue(graph.getOrCreateJobNode("3", "1").getInEdges().size() == 1);

    assertTrue(graph.getOrCreateStreamEdge(s1).getSourceNodes().size() == 0);
    assertTrue(graph.getOrCreateStreamEdge(s1).getTargetNodes().size() == 1);
    assertTrue(graph.getOrCreateStreamEdge(s2).getSourceNodes().size() == 0);
    assertTrue(graph.getOrCreateStreamEdge(s2).getTargetNodes().size() == 1);
    assertTrue(graph.getOrCreateStreamEdge(s3).getSourceNodes().size() == 0);
    assertTrue(graph.getOrCreateStreamEdge(s3).getTargetNodes().size() == 2);
  }

  @Test
  public void testAddSink() {
    /**
     * 1 -> s1
     * 2 -> s2
     * 2 -> s3
     */
    OperatorSpecGraph specGraph = mock(OperatorSpecGraph.class);
    when(specGraph.getBroadcastStreams()).thenReturn(Collections.emptySet());
    JobGraph graph = new JobGraph(null, specGraph);
    JobNode n1 = graph.getOrCreateJobNode("1", "1");
    JobNode n2 = graph.getOrCreateJobNode("2", "1");
    StreamSpec s1 = genStream();
    StreamSpec s2 = genStream();
    StreamSpec s3 = genStream();
    graph.addSink(s1, n1);
    graph.addSink(s2, n2);
    graph.addSink(s3, n2);

    assertTrue(graph.getSinks().size() == 3);
    assertTrue(graph.getOrCreateJobNode("1", "1").getOutEdges().size() == 1);
    assertTrue(graph.getOrCreateJobNode("2", "1").getOutEdges().size() == 2);

    assertTrue(graph.getOrCreateStreamEdge(s1).getSourceNodes().size() == 1);
    assertTrue(graph.getOrCreateStreamEdge(s1).getTargetNodes().size() == 0);
    assertTrue(graph.getOrCreateStreamEdge(s2).getSourceNodes().size() == 1);
    assertTrue(graph.getOrCreateStreamEdge(s2).getTargetNodes().size() == 0);
    assertTrue(graph.getOrCreateStreamEdge(s3).getSourceNodes().size() == 1);
    assertTrue(graph.getOrCreateStreamEdge(s3).getTargetNodes().size() == 0);
  }

  @Test
  public void testReachable() {
    Set<JobNode> reachable1 = graph1.findReachable();
    assertTrue(reachable1.size() == 8);

    Set<JobNode> reachable2 = graph2.findReachable();
    assertTrue(reachable2.size() == 7);
  }

  @Test
  public void testTopologicalSort() {

    // test graph1
    List<JobNode> sortedNodes1 = graph1.topologicalSort();
    Map<String, Integer> idxMap1 = new HashMap<>();
    for (int i = 0; i < sortedNodes1.size(); i++) {
      idxMap1.put(sortedNodes1.get(i).getJobName(), i);
    }

    assertTrue(idxMap1.size() == 8);
    assertTrue(idxMap1.get("11") > idxMap1.get("5"));
    assertTrue(idxMap1.get("11") > idxMap1.get("7"));
    assertTrue(idxMap1.get("8") > idxMap1.get("7"));
    assertTrue(idxMap1.get("8") > idxMap1.get("3"));
    assertTrue(idxMap1.get("2") > idxMap1.get("11"));
    assertTrue(idxMap1.get("9") > idxMap1.get("8"));
    assertTrue(idxMap1.get("9") > idxMap1.get("11"));
    assertTrue(idxMap1.get("10") > idxMap1.get("11"));
    assertTrue(idxMap1.get("10") > idxMap1.get("3"));

    // test graph2
    List<JobNode> sortedNodes2 = graph2.topologicalSort();
    Map<String, Integer> idxMap2 = new HashMap<>();
    for (int i = 0; i < sortedNodes2.size(); i++) {
      idxMap2.put(sortedNodes2.get(i).getJobName(), i);
    }

    assertTrue(idxMap2.size() == 7);
    assertTrue(idxMap2.get("2") > idxMap2.get("1"));
    assertTrue(idxMap2.get("3") > idxMap2.get("1"));
    assertTrue(idxMap2.get("4") > idxMap2.get("1"));
    assertTrue(idxMap2.get("6") > idxMap2.get("1"));
    assertTrue(idxMap2.get("5") > idxMap2.get("4"));
    assertTrue(idxMap2.get("7") > idxMap2.get("5"));

    //test graph3
    List<JobNode> sortedNodes3 = graph3.topologicalSort();
    assertTrue(sortedNodes3.size() == 2);
    assertEquals(sortedNodes3.get(0).getJobName(), "1");
    assertEquals(sortedNodes3.get(1).getJobName(), "2");

    //test graph4
    List<JobNode> sortedNodes4 = graph4.topologicalSort();
    assertTrue(sortedNodes4.size() == 1);
    assertEquals(sortedNodes4.get(0).getJobName(), "1");
  }
}
