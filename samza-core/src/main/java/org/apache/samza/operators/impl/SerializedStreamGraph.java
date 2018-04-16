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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.SamzaException;
import org.apache.samza.operators.ContextManager;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.system.StreamSpec;

import static com.google.common.base.Preconditions.*;


/**
 * Defines the serialized format of {@link StreamGraphImpl}. This class encapsulates all getter methods to get the {@link OperatorSpec}
 * initialized in the {@link StreamGraphImpl} and constructsthe corresponding serialized instances of {@link OperatorSpec}.
 * The {@link StreamGraphImpl} and {@link OperatorSpec} instances included in this class are considered as immutable and read-only.
 * The instance of {@link SerializedStreamGraph} should only be used in runtime to construct {@link org.apache.samza.task.StreamOperatorTask}.
 */
public class SerializedStreamGraph {
  private final Map<String, OperatorSpec> operatorSpecMap = new HashMap<>();
  private final Map<String, byte[]> serializedOpSpecs = new ConcurrentHashMap<>();
  private final StreamGraphImpl originalGraph;

  public SerializedStreamGraph(StreamGraphImpl streamGraph) {
    this.originalGraph = streamGraph;
    streamGraph.getAllOperatorSpecs().stream().forEach(opSpec -> this.operatorSpecMap.put(opSpec.getOpId(), opSpec));
  }

  public OperatorSpec getOpSpec(String opId) throws IOException, ClassNotFoundException {

    return OperatorSpec.fromByte(this.serializedOpSpecs.computeIfAbsent(opId, operatorId -> {
        checkNotNull(this.operatorSpecMap.get(opId), String.format("Input operator %s does not exist in serialized user program.", opId));
        try {
          return OperatorSpec.toByte(this.operatorSpecMap.get(operatorId));
        } catch (IOException e) {
          throw new SamzaException(String.format("Failed to serialize operator %s.", opId), e);
        }
      }));
  }

  public ContextManager getContextManager() {
    return this.originalGraph.getContextManager();
  }

  public Map<StreamSpec, InputOperatorSpec> getInputOperators() {
    return Collections.unmodifiableMap(this.originalGraph.getInputOperators());
  }

  public Map<StreamSpec, OutputStreamImpl> getOutputStreams() {
    return Collections.unmodifiableMap(this.originalGraph.getOutputStreams());
  }

}
