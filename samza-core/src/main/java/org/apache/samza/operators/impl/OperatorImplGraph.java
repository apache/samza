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

import com.google.common.collect.Lists;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.spec.InputOperatorSpec;
import org.apache.samza.operators.spec.JoinOperatorSpec;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.OutputOperatorSpec;
import org.apache.samza.operators.spec.PartitionByOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.util.InternalInMemoryStore;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * The DAG of {@link OperatorImpl}s corresponding to the DAG of {@link OperatorSpec}s.
 */
public class OperatorImplGraph {

  /**
   * A mapping from operator names to their {@link OperatorImpl}s in this graph. Used to avoid creating
   * multiple {@link OperatorImpl}s for an {@link OperatorSpec} when it's reached from different
   * {@link OperatorSpec}s during DAG traversals (e.g., for the merge operator).
   * We use a LHM for deterministic ordering in initializing and closing operators.
   */
  private final Map<String, OperatorImpl> operatorImpls = new LinkedHashMap<>();

  /**
   * A mapping from input {@link SystemStream}s to their {@link InputOperatorImpl} sub-DAG in this graph.
   */
  private final Map<SystemStream, InputOperatorImpl> inputOperators = new HashMap<>();

  /**
   * A mapping from {@link JoinOperatorSpec}s to their two {@link PartialJoinFunction}s. Used to associate
   * the two {@link PartialJoinOperatorImpl}s for a {@link JoinOperatorSpec} with each other since they're
   * reached from different {@link OperatorSpec} during DAG traversals.
   */
  private final Map<Integer, KV<PartialJoinFunction, PartialJoinFunction>> joinFunctions = new HashMap<>();

  private final Clock clock;

  /**
   * Constructs the DAG of {@link OperatorImpl}s corresponding to the the DAG of {@link OperatorSpec}s
   * in the {@code streamGraph}.
   *
   * @param streamGraph  the {@link StreamGraphImpl} containing the logical {@link OperatorSpec} DAG
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @param clock  the {@link Clock} to get current time
   */
  public OperatorImplGraph(StreamGraphImpl streamGraph, Config config, TaskContext context, Clock clock) {
    this.clock = clock;
    streamGraph.getInputOperators().forEach((streamSpec, inputOpSpec) -> {
        SystemStream systemStream = new SystemStream(streamSpec.getSystemName(), streamSpec.getPhysicalName());
        InputOperatorImpl inputOperatorImpl =
            (InputOperatorImpl) createAndRegisterOperatorImpl(null, inputOpSpec, config, context);
        this.inputOperators.put(systemStream, inputOperatorImpl);
      });
  }

  /**
   * Get the {@link InputOperatorImpl} corresponding to the provided input {@code systemStream}.
   *
   * @param systemStream  input {@link SystemStream}
   * @return  the {@link InputOperatorImpl} that starts processing the input message
   */
  public InputOperatorImpl getInputOperator(SystemStream systemStream) {
    return this.inputOperators.get(systemStream);
  }

  public void close() {
    List<OperatorImpl> initializationOrder = new ArrayList<>(operatorImpls.values());
    List<OperatorImpl> finalizationOrder = Lists.reverse(initializationOrder);
    finalizationOrder.forEach(OperatorImpl::close);
  }

  /**
   * Get all {@link InputOperatorImpl}s for the graph.
   *
   * @return  an unmodifiable view of all {@link InputOperatorImpl}s for the graph
   */
  public Collection<InputOperatorImpl> getAllInputOperators() {
    return Collections.unmodifiableCollection(this.inputOperators.values());
  }

  /**
   * Traverses the DAG of {@link OperatorSpec}s starting from the provided {@link OperatorSpec},
   * creates the corresponding DAG of {@link OperatorImpl}s, and returns the root {@link OperatorImpl} node.
   *
   * @param prevOperatorSpec  the parent of the current {@code operatorSpec} in the traversal
   * @param operatorSpec  the operatorSpec to create the {@link OperatorImpl} for
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @return  the operator implementation for the operatorSpec
   */
  OperatorImpl createAndRegisterOperatorImpl(OperatorSpec prevOperatorSpec, OperatorSpec operatorSpec,
      Config config, TaskContext context) {
    if (!operatorImpls.containsKey(operatorSpec.getOpName()) || operatorSpec instanceof JoinOperatorSpec) {
      // Either this is the first time we've seen this operatorSpec, or this is a join operator spec
      // and we need to create 2 partial join operator impls for it. Initialize and register the sub-DAG.
      OperatorImpl operatorImpl = createOperatorImpl(prevOperatorSpec, operatorSpec, config, context);
      operatorImpl.init(config, context);
      operatorImpls.put(operatorImpl.getOperatorName(), operatorImpl);

      Collection<OperatorSpec> registeredSpecs = operatorSpec.getRegisteredOperatorSpecs();
      registeredSpecs.forEach(registeredSpec -> {
          OperatorImpl nextImpl = createAndRegisterOperatorImpl(operatorSpec, registeredSpec, config, context);
          operatorImpl.registerNextOperator(nextImpl);
        });
      return operatorImpl;
    } else {
      // the implementation corresponding to operatorSpec has already been instantiated
      // and registered, so we do not need to traverse the DAG further.
      return operatorImpls.get(operatorSpec.getOpName());
    }
  }

  /**
   * Creates a new {@link OperatorImpl} instance for the provided {@link OperatorSpec}.
   *
   * @param operatorSpec  the immutable {@link OperatorSpec} definition.
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @return  the {@link OperatorImpl} implementation instance
   */
  OperatorImpl createOperatorImpl(OperatorSpec prevOperatorSpec, OperatorSpec operatorSpec,
      Config config, TaskContext context) {
    if (operatorSpec instanceof InputOperatorSpec) {
      return new InputOperatorImpl((InputOperatorSpec) operatorSpec);
    } else if (operatorSpec instanceof StreamOperatorSpec) {
      return new StreamOperatorImpl((StreamOperatorSpec) operatorSpec, config, context);
    } else if (operatorSpec instanceof SinkOperatorSpec) {
      return new SinkOperatorImpl((SinkOperatorSpec) operatorSpec, config, context);
    } else if (operatorSpec instanceof OutputOperatorSpec) {
      return new OutputOperatorImpl((OutputOperatorSpec) operatorSpec, config, context);
    } else if (operatorSpec instanceof PartitionByOperatorSpec) {
      return new PartitionByOperatorImpl((PartitionByOperatorSpec) operatorSpec, config, context);
    } else if (operatorSpec instanceof WindowOperatorSpec) {
      return new WindowOperatorImpl((WindowOperatorSpec) operatorSpec, clock);
    } else if (operatorSpec instanceof JoinOperatorSpec) {
      return createPartialJoinOperatorImpl(prevOperatorSpec, (JoinOperatorSpec) operatorSpec, config, context, clock);
    }
    throw new IllegalArgumentException(
        String.format("Unsupported OperatorSpec: %s", operatorSpec.getClass().getName()));
  }

  private PartialJoinOperatorImpl createPartialJoinOperatorImpl(OperatorSpec prevOperatorSpec,
      JoinOperatorSpec joinOpSpec, Config config, TaskContext context, Clock clock) {
    KV<PartialJoinFunction, PartialJoinFunction> partialJoinFunctions = getOrCreatePartialJoinFunctions(joinOpSpec);
    if (joinOpSpec.getLeftInputOpSpec().equals(prevOperatorSpec)) { // we got here from the left side of the join
      return new PartialJoinOperatorImpl(joinOpSpec, /* isLeftSide */ true,
          partialJoinFunctions.getKey(), partialJoinFunctions.getValue(), config, context, clock);
    } else { // we got here from the right side of the join
      return new PartialJoinOperatorImpl(joinOpSpec, /* isLeftSide */ false,
          partialJoinFunctions.getValue(), partialJoinFunctions.getKey(), config, context, clock);
    }
  }

  private KV<PartialJoinFunction, PartialJoinFunction> getOrCreatePartialJoinFunctions(JoinOperatorSpec joinOpSpec) {
    return joinFunctions.computeIfAbsent(joinOpSpec.getOpId(),
        joinOpId -> KV.of(createLeftJoinFn(joinOpSpec.getJoinFn()), createRightJoinFn(joinOpSpec.getJoinFn())));
  }

  private PartialJoinFunction<Object, Object, Object, Object> createLeftJoinFn(JoinFunction joinFn) {
    return new PartialJoinFunction<Object, Object, Object, Object>() {
      private KeyValueStore<Object, PartialJoinMessage<Object>> leftStreamState = new InternalInMemoryStore<>();

      @Override
      public Object apply(Object m, Object jm) {
        return joinFn.apply(m, jm);
      }

      @Override
      public Object getKey(Object message) {
        return joinFn.getFirstKey(message);
      }

      @Override
      public KeyValueStore<Object, PartialJoinMessage<Object>> getState() {
        return leftStreamState;
      }

      @Override
      public void init(Config config, TaskContext context) {
        // user-defined joinFn should only be initialized once, so we do it only in left partial join function.
        joinFn.init(config, context);
      }

      @Override
      public void close() {
        // joinFn#close() must only be called once, so we do it it only in left partial join function.
        joinFn.close();
      }
    };
  }

  private PartialJoinFunction<Object, Object, Object, Object> createRightJoinFn(JoinFunction joinFn) {
    return new PartialJoinFunction<Object, Object, Object, Object>() {
      private KeyValueStore<Object, PartialJoinMessage<Object>> rightStreamState = new InternalInMemoryStore<>();

      @Override
      public Object apply(Object m, Object jm) {
        return joinFn.apply(jm, m);
      }

      @Override
      public Object getKey(Object message) {
        return joinFn.getSecondKey(message);
      }

      @Override
      public KeyValueStore<Object, PartialJoinMessage<Object>> getState() {
        return rightStreamState;
      }
    };
  }
}
