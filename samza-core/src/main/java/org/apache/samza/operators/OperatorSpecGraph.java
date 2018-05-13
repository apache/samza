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
package org.apache.samza.operators;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputStreamImpl;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.table.TableSpec;


/**
 * Defines the serialized format of {@link StreamGraphSpec}. This class encapsulates all getter methods to get the {@link OperatorSpec}
 * initialized in the {@link StreamGraphSpec} and constructsthe corresponding serialized instances of {@link OperatorSpec}.
 * The {@link StreamGraphSpec} and {@link OperatorSpec} instances included in this class are considered as immutable and read-only.
 * The instance of {@link OperatorSpecGraph} should only be used in runtime to construct {@link org.apache.samza.task.StreamOperatorTask}.
 */
public class OperatorSpecGraph implements Serializable {
  // We use a LHM for deterministic order in initializing and closing operators.
  private final Map<StreamSpec, InputOperatorSpec> inputOperators;
  private final Map<StreamSpec, OutputStreamImpl> outputStreams;
  private final Map<TableSpec, TableImpl> tables;
  private final Set<OperatorSpec> allOpSpecs;
  private final boolean hasWindowOrJoins;

  // The following objects are transient since they are recreateable.
  private transient SerializableSerde<OperatorSpecGraph> graphSpecSerde = new SerializableSerde<>();
  private transient final byte[] serializedGraphSpec;

  private HashSet<OperatorSpec> findAllOperatorSpecs() {
    Collection<InputOperatorSpec> inputOperatorSpecs = this.inputOperators.values();
    HashSet<OperatorSpec> operatorSpecs = new HashSet<>();
    for (InputOperatorSpec inputOperatorSpec : inputOperatorSpecs) {
      operatorSpecs.add(inputOperatorSpec);
      doGetOperatorSpecs(inputOperatorSpec, operatorSpecs);
    }
    return operatorSpecs;
  }

  private void doGetOperatorSpecs(OperatorSpec operatorSpec, Set<OperatorSpec> specs) {
    Collection<OperatorSpec> registeredOperatorSpecs = operatorSpec.getRegisteredOperatorSpecs();
    for (OperatorSpec registeredOperatorSpec : registeredOperatorSpecs) {
      specs.add(registeredOperatorSpec);
      doGetOperatorSpecs(registeredOperatorSpec, specs);
    }
  }

  private boolean checkWindowOrJoins() {
    Set<OperatorSpec> windowOrJoinSpecs = allOpSpecs.stream()
        .filter(spec -> spec.getOpCode() == OperatorSpec.OpCode.WINDOW || spec.getOpCode() == OperatorSpec.OpCode.JOIN)
        .collect(Collectors.toSet());

    return windowOrJoinSpecs.size() != 0;
  }

  OperatorSpecGraph(StreamGraphSpec graphSpec) {
    this.inputOperators = graphSpec.getInputOperators();
    this.outputStreams = graphSpec.getOutputStreams();
    this.tables = graphSpec.getTables();
    this.allOpSpecs = Collections.unmodifiableSet(this.findAllOperatorSpecs());
    hasWindowOrJoins = checkWindowOrJoins();
    serializedGraphSpec = graphSpecSerde.toBytes(this);
  }

  public Map<StreamSpec, InputOperatorSpec> getInputOperators() {
    return inputOperators;
  }

  public Map<StreamSpec, OutputStreamImpl> getOutputStreams() {
    return outputStreams;
  }

  public Map<TableSpec, TableImpl> getTables() {
    return tables;
  }

  /**
   * Get all {@link OperatorSpec}s available in this {@link StreamGraphSpec}
   *
   * @return all available {@link OperatorSpec}s
   */
  public Collection<OperatorSpec> getAllOperatorSpecs() {
    return allOpSpecs;
  }

  /**
   * Returns <tt>true</tt> iff this {@link StreamGraphSpec} contains a join or a window operator
   *
   * @return  <tt>true</tt> iff this {@link StreamGraphSpec} contains a join or a window operator
   */
  public boolean hasWindowOrJoins() {
    return hasWindowOrJoins;
  }

  public OperatorSpecGraph clone() {
    if (graphSpecSerde == null) {
      throw new IllegalStateException("Cannot clone from an already deserialized OperatorSpecGraph.");
    }
    return graphSpecSerde.fromBytes(serializedGraphSpec);
  }

}
