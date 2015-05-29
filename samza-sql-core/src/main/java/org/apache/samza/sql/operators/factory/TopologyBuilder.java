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
package org.apache.samza.sql.operators.factory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.OperatorRouter;
import org.apache.samza.sql.api.operators.OperatorSink;
import org.apache.samza.sql.api.operators.OperatorSource;
import org.apache.samza.sql.api.operators.OperatorSpec;
import org.apache.samza.sql.api.operators.SimpleOperator;
import org.apache.samza.sql.api.operators.SqlOperatorFactory;
import org.apache.samza.sql.operators.OperatorTopology;
import org.apache.samza.sql.operators.SimpleRouter;


/**
 * This class implements a builder to allow user to create the operators and connect them in a topology altogether.
 */
public class TopologyBuilder {

  /**
   * Internal {@link org.apache.samza.sql.api.operators.OperatorRouter} object to retain the topology being created
   */
  private SimpleRouter router;

  /**
   * The {@link org.apache.samza.sql.api.operators.SqlOperatorFactory} object used to create operators connected in the topology
   */
  private final SqlOperatorFactory factory;

  /**
   * The map of unbound inputs, the value is set(input_operators)
   */
  private Map<EntityName, Set<OperatorSpec>> unboundInputs = new HashMap<EntityName, Set<OperatorSpec>>();

  /**
   * The map of unbound outputs, the value is the operator generating the output
   */
  private Map<EntityName, OperatorSpec> unboundOutputs = new HashMap<EntityName, OperatorSpec>();

  /**
   * The set of entities that are intermediate entities between operators
   */
  private Set<EntityName> interStreams = new HashSet<EntityName>();

  /**
   * The current operator that may have unbound input or output
   */
  private SimpleOperator currentOp = null;

  /**
   * Private constructor of {@code TopologyBuilder}
   *
   * @param factory The {@link org.apache.samza.sql.api.operators.SqlOperatorFactory} to create operators
   */
  private TopologyBuilder(SqlOperatorFactory factory) {
    this.router = new SimpleRouter();
    this.factory = factory;
  }

  /**
   * Static method to create this {@code TopologyBuilder} w/ a customized {@link org.apache.samza.sql.api.operators.SqlOperatorFactory}
   *
   * @param factory The {@link org.apache.samza.sql.api.operators.SqlOperatorFactory} to create operators
   * @return The {@code TopologyBuilder} object
   */
  public static TopologyBuilder create(SqlOperatorFactory factory) {
    return new TopologyBuilder(factory);
  }

  /**
   * Static method to create this {@code TopologyBuilder}
   *
   * @return The {@code TopologyBuilder} object
   */
  public static TopologyBuilder create() {
    return new TopologyBuilder(new SimpleOperatorFactoryImpl());
  }

  /**
   * Public method to create the next operator and attach it to the output of the current operator
   *
   * @param spec The {@link org.apache.samza.sql.api.operators.OperatorSpec} for the next operator
   * @return The updated {@code TopologyBuilder} object
   */
  public TopologyBuilder operator(OperatorSpec spec) {
    // check whether it is valid to connect a new operator to the current operator's output
    SimpleOperator nextOp = this.factory.getOperator(spec);
    return this.operator(nextOp);
  }

  /**
   * Public method to create the next operator and attach it to the output of the current operator
   *
   * @param op The {@link org.apache.samza.sql.api.operators.SimpleOperator}
   * @return The updated {@code TopologyBuilder} object
   */
  public TopologyBuilder operator(SimpleOperator op) {
    // check whether it is valid to connect a new operator to the current operator's output
    canAddOperator(op);
    this.addOperator(op);
    // advance the current operator position
    this.currentOp = op;
    return this;
  }

  /**
   * Public method to create a stream object that will be the source to other operators
   *
   * @return The {@link org.apache.samza.sql.api.operators.OperatorSource} that can be the source to other operators
   */
  public OperatorSource stream() {
    canCreateSource();
    return new OperatorTopology(this.unboundOutputs.keySet().iterator().next(), this.router);
  }

  /**
   * Public method to create a sink object that can take input stream from other operators
   *
   * @return The {@link org.apache.samza.sql.api.operators.OperatorSink} that can be the downstream of other operators
   */
  public OperatorSink sink() {
    canCreateSink();
    return new OperatorTopology(this.unboundInputs.keySet().iterator().next(), this.router);
  }

  /**
   * Public method to bind the input of the current operator w/ the {@link org.apache.samza.sql.api.operators.OperatorSource} object
   *
   * @param srcStream The {@link org.apache.samza.sql.api.operators.OperatorSource} that the current operator is going to be bound to
   * @return The updated {@code TopologyBuilder} object
   */
  public TopologyBuilder bind(OperatorSource srcStream) {
    EntityName streamName = srcStream.getName();
    if (this.unboundInputs.containsKey(streamName)) {
      this.unboundInputs.remove(streamName);
      this.interStreams.add(streamName);
    } else {
      // no input operator is waiting for the output from the srcStream
      throw new IllegalArgumentException("No operator input can be bound to the input stream " + streamName);
    }
    // add all operators in srcStream to this topology
    for (Iterator<SimpleOperator> iter = srcStream.opIterator(); iter.hasNext();) {
      this.addOperator(iter.next());
    }
    return this;
  }

  /**
   * Public method to attach a {@link org.apache.samza.sql.api.operators.OperatorSink} object to the output of the current operator
   *
   * @param nextSink The {@link org.apache.samza.sql.api.operators.OperatorSink} to be attached to the current operator's output
   * @return The updated {@code TopologyBuilder} object
   */
  public TopologyBuilder attach(OperatorSink nextSink) {
    EntityName streamName = nextSink.getName();
    if (this.unboundOutputs.containsKey(streamName)) {
      this.unboundOutputs.remove(streamName);
      this.interStreams.add(streamName);
    } else {
      // no unbound output to attach to
      throw new IllegalArgumentException("No operator output found to attach the sink " + streamName);
    }
    // add all operators in nextSink to the router
    for (Iterator<SimpleOperator> iter = nextSink.opIterator(); iter.hasNext();) {
      this.addOperator(iter.next());
    }
    return this;
  }

  /**
   * Public method to finalize the topology that should have all input and output bound to system input and output
   *
   * @return The finalized {@link org.apache.samza.sql.api.operators.OperatorRouter} object
   */
  public OperatorRouter build() {
    canClose();
    return router;
  }

  private TopologyBuilder addOperator(SimpleOperator nextOp) {
    // if input is not in the unboundOutputs and interStreams, input is unbound
    for (EntityName in : nextOp.getSpec().getInputNames()) {
      if (this.unboundOutputs.containsKey(in)) {
        this.unboundOutputs.remove(in);
        this.interStreams.add(in);
      }
      if (!this.interStreams.contains(in) && !in.isSystemEntity()) {
        if (!this.unboundInputs.containsKey(in)) {
          this.unboundInputs.put(in, new HashSet<OperatorSpec>());
        }
        this.unboundInputs.get(in).add(nextOp.getSpec());
      }
    }
    // if output is not in the unboundInputs and interStreams, output is unbound
    for (EntityName out : nextOp.getSpec().getOutputNames()) {
      if (this.unboundInputs.containsKey(out)) {
        this.unboundInputs.remove(out);
        this.interStreams.add(out);
      }
      if (!this.interStreams.contains(out) && !out.isSystemEntity()) {
        this.unboundOutputs.put(out, nextOp.getSpec());
      }
    }
    try {
      this.router.addOperator(nextOp);
    } catch (Exception e) {
      throw new RuntimeException("Failed to add operator " + nextOp.getSpec().getId() + " to the topology.", e);
    }
    return this;
  }

  private void canCreateSource() {
    if (this.unboundInputs.size() > 0) {
      throw new IllegalStateException("Can't create stream when there are unbounded input streams in the topology");
    }
    if (this.unboundOutputs.size() != 1) {
      throw new IllegalStateException(
          "Can't create stream when the number of unbounded outputs is not 1 in the topology");
    }
  }

  private void canCreateSink() {
    if (this.unboundOutputs.size() > 0) {
      throw new IllegalStateException("Can't create sink when there are unbounded output streams in the topology");
    }
    if (this.unboundInputs.size() != 1) {
      throw new IllegalStateException(
          "Can't create sink when the number of unbounded input streams is not 1 in the topology");
    }
  }

  private void canAddOperator(SimpleOperator op) {
    if (this.currentOp == null) {
      return;
    }
    for (EntityName name : this.currentOp.getSpec().getInputNames()) {
      if (this.unboundInputs.containsKey(name)) {
        throw new IllegalArgumentException("There are unbound input " + name + " to the current operator "
            + this.currentOp.getSpec().getId() + ". Create a sink or call bind instead");
      }
    }
    List<EntityName> nextInputs = op.getSpec().getInputNames();
    for (EntityName name : this.currentOp.getSpec().getOutputNames()) {
      if (!nextInputs.contains(name) && this.unboundOutputs.containsKey(name)) {
        // the current operator's output is not in the next operator's input list
        throw new IllegalArgumentException("There are unbound output " + name + " from the current operator "
            + this.currentOp.getSpec().getId()
            + " that are not included in the next operator's inputs. Create a stream or call attach instead");
      }
    }
  }

  private void canClose() {
    if (!this.unboundInputs.isEmpty() || !this.unboundOutputs.isEmpty()) {
      throw new IllegalStateException(
          "There are input/output streams in the topology that are not bounded. Can't build the topology yet.");
    }
  }

}
