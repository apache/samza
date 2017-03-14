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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.system.StreamSpec;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


public class TestProcessorGraph {

  ProcessorGraph graph1;
  ProcessorGraph graph2;
  int streamSeq = 0;

  private StreamSpec genStream() {
    ++streamSeq;

    return new StreamSpec(String.valueOf(streamSeq), "test-stream", "test-system");
  }

  @Before
  public void setup() {
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
    // init graph1
    graph1 = new ProcessorGraph(null);
    graph1.addSource(genStream(), "5");
    graph1.addSource(genStream(), "7");
    graph1.addSource(genStream(), "3");
    graph1.addIntermediateStream(genStream(), "5", "11");
    graph1.addIntermediateStream(genStream(), "7", "11");
    graph1.addIntermediateStream(genStream(), "7", "8");
    graph1.addIntermediateStream(genStream(), "3", "8");
    graph1.addIntermediateStream(genStream(), "11", "2");
    graph1.addIntermediateStream(genStream(), "11", "9");
    graph1.addIntermediateStream(genStream(), "8", "9");
    graph1.addIntermediateStream(genStream(), "11", "10");
    graph1.addSink(genStream(), "2");
    graph1.addSink(genStream(), "9");
    graph1.addSink(genStream(), "10");

    /**
     * graph2 is a graph with a loop
     * 1 -> 2 -> 3 -> 4 -> 5 -> 7
     *      |<---6 <--|    <>
     */
    graph2 = new ProcessorGraph(null);
    graph2.addSource(genStream(), "1");
    graph2.addIntermediateStream(genStream(), "1", "2");
    graph2.addIntermediateStream(genStream(), "2", "3");
    graph2.addIntermediateStream(genStream(), "3", "4");
    graph2.addIntermediateStream(genStream(), "4", "5");
    graph2.addIntermediateStream(genStream(), "4", "6");
    graph2.addIntermediateStream(genStream(), "6", "2");
    graph2.addIntermediateStream(genStream(), "5", "5");
    graph2.addIntermediateStream(genStream(), "5", "7");
    graph2.addSink(genStream(), "7");
  }

  @Test
  public void testAddSource() {
    ProcessorGraph graph = new ProcessorGraph(null);

    /**
     * s1 -> 1
     * s2 ->|
     *
     * s3 -> 2
     *   |-> 3
     */
    StreamSpec s1 = genStream();
    StreamSpec s2 = genStream();
    StreamSpec s3 = genStream();
    graph.addSource(s1, "1");
    graph.addSource(s2, "1");
    graph.addSource(s3, "2");
    graph.addSource(s3, "3");

    assertTrue(graph.getSources().size() == 3);

    assertTrue(graph.getOrCreateProcessor("1").getInEdges().size() == 2);
    assertTrue(graph.getOrCreateProcessor("2").getInEdges().size() == 1);
    assertTrue(graph.getOrCreateProcessor("3").getInEdges().size() == 1);

    assertTrue(graph.getOrCreateEdge(s1).getSourceNodes().size() == 0);
    assertTrue(graph.getOrCreateEdge(s1).getTargetNodes().size() == 1);
    assertTrue(graph.getOrCreateEdge(s2).getSourceNodes().size() == 0);
    assertTrue(graph.getOrCreateEdge(s2).getTargetNodes().size() == 1);
    assertTrue(graph.getOrCreateEdge(s3).getSourceNodes().size() == 0);
    assertTrue(graph.getOrCreateEdge(s3).getTargetNodes().size() == 2);
  }

  @Test
  public void testAddSink() {
    /**
     * 1 -> s1
     * 2 -> s2
     * 2 -> s3
     */
    StreamSpec s1 = genStream();
    StreamSpec s2 = genStream();
    StreamSpec s3 = genStream();
    ProcessorGraph graph = new ProcessorGraph(null);
    graph.addSink(s1, "1");
    graph.addSink(s2, "2");
    graph.addSink(s3, "2");

    assertTrue(graph.getSinks().size() == 3);
    assertTrue(graph.getOrCreateProcessor("1").getOutEdges().size() == 1);
    assertTrue(graph.getOrCreateProcessor("2").getOutEdges().size() == 2);

    assertTrue(graph.getOrCreateEdge(s1).getSourceNodes().size() == 1);
    assertTrue(graph.getOrCreateEdge(s1).getTargetNodes().size() == 0);
    assertTrue(graph.getOrCreateEdge(s2).getSourceNodes().size() == 1);
    assertTrue(graph.getOrCreateEdge(s2).getTargetNodes().size() == 0);
    assertTrue(graph.getOrCreateEdge(s3).getSourceNodes().size() == 1);
    assertTrue(graph.getOrCreateEdge(s3).getTargetNodes().size() == 0);
  }

  @Test
  public void testReachable() {
    Set<ProcessorNode> reachable1 = graph1.findReachable();
    assertTrue(reachable1.size() == 8);

    Set<ProcessorNode> reachable2 = graph2.findReachable();
    assertTrue(reachable2.size() == 7);
  }

  @Test
  public void testTopologicalSort() {

    // test graph1
    List<ProcessorNode> sortedNodes1 = graph1.topologicalSort();
    Map<String, Integer> idxMap1 = new HashMap<>();
    for (int i = 0; i < sortedNodes1.size(); i++) {
      idxMap1.put(sortedNodes1.get(i).getId(), i);
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
    List<ProcessorNode> sortedNodes2 = graph2.topologicalSort();
    Map<String, Integer> idxMap2 = new HashMap<>();
    for (int i = 0; i < sortedNodes2.size(); i++) {
      idxMap2.put(sortedNodes2.get(i).getId(), i);
    }

    assertTrue(idxMap2.size() == 7);
    assertTrue(idxMap2.get("2") > idxMap2.get("1"));
    assertTrue(idxMap2.get("3") > idxMap2.get("1"));
    assertTrue(idxMap2.get("4") > idxMap2.get("1"));
    assertTrue(idxMap2.get("6") > idxMap2.get("1"));
    assertTrue(idxMap2.get("5") > idxMap2.get("4"));
    assertTrue(idxMap2.get("7") > idxMap2.get("5"));
  }
}
