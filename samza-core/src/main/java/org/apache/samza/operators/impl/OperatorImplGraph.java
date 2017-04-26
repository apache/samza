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
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.TaskContext;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Instantiates the DAG of {@link OperatorImpl}s corresponding to the {@link OperatorSpec}s for the input
 * {@link MessageStreamImpl}s.
 */
public class OperatorImplGraph {

  /**
   * A mapping from {@link OperatorSpec}s to their {@link OperatorImpl}s in this graph. Used to avoid creating
   * multiple {@link OperatorImpl}s for an {@link OperatorSpec}, e.g., when it's reached from different
   * input {@link MessageStreamImpl}s.
   */
  private final Map<OperatorSpec, OperatorImpl> operatorImpls = new HashMap<>();

  /**
   * A mapping from input {@link SystemStream}s to their {@link OperatorImpl} sub-DAG in this graph.
   */
  private final Map<SystemStream, RootOperatorImpl> rootOperators = new HashMap<>();

  private final Clock clock;

  public OperatorImplGraph(Clock clock) {
    this.clock = clock;
  }

  /* package private */ OperatorImplGraph() {
    this(SystemClock.instance());
  }

  /**
   * Initialize the DAG of {@link OperatorImpl}s for the input {@link MessageStreamImpl} in the provided
   * {@link StreamGraphImpl}.
   *
   * @param streamGraph  the logical {@link StreamGraphImpl}
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   */
  public void init(StreamGraphImpl streamGraph, Config config, TaskContext context) {
    streamGraph.getInputStreams().forEach((streamSpec, inputStream) -> {
        SystemStream systemStream = new SystemStream(streamSpec.getSystemName(), streamSpec.getPhysicalName());
        this.rootOperators.put(systemStream, this.createOperatorImpls((MessageStreamImpl) inputStream, config, context));
      });
  }

  /**
   * Get the {@link RootOperatorImpl} corresponding to the provided input {@code systemStream}.
   *
   * @param systemStream  input {@link SystemStream}
   * @return  the {@link RootOperatorImpl} that starts processing the input message
   */
  public RootOperatorImpl getRootOperator(SystemStream systemStream) {
    return this.rootOperators.get(systemStream);
  }

  /**
   * Get all {@link RootOperatorImpl}s for the graph.
   *
   * @return  an unmodifiable view of all {@link RootOperatorImpl}s for the graph
   */
  public Collection<RootOperatorImpl> getAllRootOperators() {
    return Collections.unmodifiableCollection(this.rootOperators.values());
  }

  /**
   * Traverses the DAG of {@link OperatorSpec}s starting from the provided {@link MessageStreamImpl},
   * creates the corresponding DAG of {@link OperatorImpl}s, and returns its root {@link RootOperatorImpl} node.
   *
   * @param source  the input {@link MessageStreamImpl} to instantiate {@link OperatorImpl}s for
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @param <M>  the type of messages in the {@code source} {@link MessageStreamImpl}
   * @return  root node for the {@link OperatorImpl} DAG
   */
  private <M> RootOperatorImpl<M> createOperatorImpls(MessageStreamImpl<M> source,
      Config config, TaskContext context) {
    // since the source message stream might have multiple operator specs registered on it,
    // create a new root node as a single point of entry for the DAG.
    RootOperatorImpl<M> rootOperator = new RootOperatorImpl<>();
    rootOperator.init(config, context);
    // create the pipeline/topology starting from the source
    source.getRegisteredOperatorSpecs().forEach(registeredOperator -> {
        // pass in the context so that operator implementations can initialize their functions
        OperatorImpl<M, ?> operatorImpl =
            createAndRegisterOperatorImpl(registeredOperator, config, context);
        rootOperator.registerNextOperator(operatorImpl);
      });
    return rootOperator;
  }

  /**
   * Helper method to recursively traverse the {@link OperatorSpec} DAG and instantiate and link the corresponding
   * {@link OperatorImpl}s.
   *
   * @param operatorSpec  the operatorSpec to create the {@link OperatorImpl} for
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @param <M>  type of input message
   * @return  the operator implementation for the operatorSpec
   */
  private <M> OperatorImpl<M, ?> createAndRegisterOperatorImpl(OperatorSpec operatorSpec,
      Config config, TaskContext context) {
    if (!operatorImpls.containsKey(operatorSpec)) {
      OperatorImpl<M, ?> operatorImpl = createOperatorImpl(operatorSpec, config, context);
      if (operatorImpls.putIfAbsent(operatorSpec, operatorImpl) == null) {
        // this is the first time we've added the operatorImpl corresponding to the operatorSpec,
        // so traverse and initialize and register the rest of the DAG.
        // initialize the corresponding operator function
        operatorImpl.init(config, context);
        MessageStreamImpl nextStream = operatorSpec.getNextStream();
        if (nextStream != null) {
          Collection<OperatorSpec> registeredSpecs = nextStream.getRegisteredOperatorSpecs();
          registeredSpecs.forEach(registeredSpec -> {
              OperatorImpl subImpl = createAndRegisterOperatorImpl(registeredSpec, config, context);
              operatorImpl.registerNextOperator(subImpl);
            });
        }
        return operatorImpl;
      }
    }

    // the implementation corresponding to operatorSpec has already been instantiated
    // and registered, so we do not need to traverse the DAG further.
    return operatorImpls.get(operatorSpec);
  }

  /**
   * Creates a new {@link OperatorImpl} instance for the provided {@link OperatorSpec}.
   *
   * @param operatorSpec  the immutable {@link OperatorSpec} definition.
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @param <M>  type of input message
   * @return  the {@link OperatorImpl} implementation instance
   */
  private <M> OperatorImpl<M, ?> createOperatorImpl(OperatorSpec operatorSpec, Config config, TaskContext context) {
    if (operatorSpec instanceof StreamOperatorSpec) {
      StreamOperatorSpec<M, ?> streamOpSpec = (StreamOperatorSpec<M, ?>) operatorSpec;
      return new StreamOperatorImpl<>(streamOpSpec, config, context);
    } else if (operatorSpec instanceof SinkOperatorSpec) {
      return new SinkOperatorImpl<>((SinkOperatorSpec<M>) operatorSpec, config, context);
    } else if (operatorSpec instanceof WindowOperatorSpec) {
      return new WindowOperatorImpl((WindowOperatorSpec<M, ?, ?>) operatorSpec, clock);
    } else if (operatorSpec instanceof PartialJoinOperatorSpec) {
      return new PartialJoinOperatorImpl<>((PartialJoinOperatorSpec) operatorSpec, config, context, clock);
    }
    throw new IllegalArgumentException(
        String.format("Unsupported OperatorSpec: %s", operatorSpec.getClass().getName()));
  }
}
