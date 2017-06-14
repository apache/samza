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

package org.apache.samza.operators.util;

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

public class IOGraphUtil {

  public static final class IONode {
    private final Set<StreamSpec> inputs = new HashSet<>();
    private final StreamSpec output;
    private final OutputOperatorSpec outputOpSpec;

    IONode(StreamSpec output, OutputOperatorSpec outputOpSpec) {
      this.output = output;
      this.outputOpSpec = outputOpSpec;
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

    public OutputOperatorSpec getOutputOpSpec() {
      return outputOpSpec;
    }
  }

  public static Collection<IONode> buildIOGraph(StreamGraphImpl streamGraph) {
    Map<Integer, IONode> ioGraph = new HashMap<>();
    streamGraph.getInputOperators().entrySet().stream()
        .forEach(entry -> buildIONodes(entry.getKey(), entry.getValue(), ioGraph));
    return Collections.unmodifiableCollection(ioGraph.values());
  }

  /* package private */
  static void buildIONodes(StreamSpec input, OperatorSpec opSpec, Map<Integer, IONode> ioGraph) {
    if (opSpec instanceof OutputOperatorSpec) {
      OutputOperatorSpec outputOpSpec = (OutputOperatorSpec) opSpec;
      IONode node = ioGraph.get(opSpec.getOpId());
      if (node == null) {
        StreamSpec output = outputOpSpec.getOutputStream().getStreamSpec();
        node = new IONode(output, outputOpSpec);
        ioGraph.put(opSpec.getOpId(), node);
      }
      node.addInput(input);
    }

    Collection<OperatorSpec> nextOperators = opSpec.getRegisteredOperatorSpecs();
    nextOperators.forEach(spec -> buildIONodes(input, spec, ioGraph));
  }
}
