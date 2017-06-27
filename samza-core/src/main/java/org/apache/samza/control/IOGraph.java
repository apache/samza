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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStream;


/**
 * This class provides the topology of stream inputs to outputs.
 */
public class IOGraph {

  public static final class IONode {
    private final Set<StreamSpec> inputs = new HashSet<>();
    private final StreamSpec output;
    private final boolean isOutputIntermediate;

    IONode(StreamSpec output, boolean isOutputIntermediate) {
      this.output = output;
      this.isOutputIntermediate = isOutputIntermediate;
    }

    void addInput(StreamSpec input) {
      inputs.add(input);
    }

    public Set<StreamSpec> getInputs() {
      return Collections.unmodifiableSet(inputs);
    }

    public StreamSpec getOutput() {
      return output;
    }

    public boolean isOutputIntermediate() {
      return isOutputIntermediate;
    }
  }

  Collection<IONode> nodes;
  Multimap<SystemStream, IONode> inputToNodes;

  public IOGraph(Collection<IONode> nodes) {
    this.nodes = Collections.unmodifiableCollection(nodes);
    this.inputToNodes = HashMultimap.create();
    nodes.forEach(node -> {
        node.getInputs().forEach(stream -> {
            inputToNodes.put(new SystemStream(stream.getSystemName(), stream.getPhysicalName()), node);
          });
      });
  }

  public Collection<IONode> getNodes() {
    return this.nodes;
  }

  public Collection<IONode> getNodesOfInput(SystemStream input) {
    return inputToNodes.get(input);
  }

  public static IOGraph buildIOGraph(StreamGraphImpl streamGraph) {
    Map<Integer, IONode> nodes = new HashMap<>();
    streamGraph.getInputOperators().entrySet().stream()
        .forEach(entry -> buildIONodes(entry.getKey(), entry.getValue(), nodes));
    return new IOGraph(nodes.values());
  }

  /* package private */
  static void buildIONodes(StreamSpec input, OperatorSpec opSpec, Map<Integer, IONode> ioGraph) {
    if (opSpec instanceof OutputOperatorSpec) {
      OutputOperatorSpec outputOpSpec = (OutputOperatorSpec) opSpec;
      IONode node = ioGraph.get(opSpec.getOpId());
      if (node == null) {
        StreamSpec output = outputOpSpec.getOutputStream().getStreamSpec();
        node = new IONode(output, outputOpSpec.getOpCode() == OperatorSpec.OpCode.PARTITION_BY);
        ioGraph.put(opSpec.getOpId(), node);
      }
      node.addInput(input);
    }

    Collection<OperatorSpec> nextOperators = opSpec.getRegisteredOperatorSpecs();
    nextOperators.forEach(spec -> buildIONodes(input, spec, ioGraph));
  }
}
