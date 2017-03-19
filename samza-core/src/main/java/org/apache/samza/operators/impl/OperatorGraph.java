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
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
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
 * Instantiates the DAG of {@link OperatorImpl}s corresponding to the {@link OperatorSpec}s for a
 * {@link MessageStreamImpl}
 */
public class OperatorGraph {

  /**
   * A {@link Map} from {@link OperatorSpec} to {@link OperatorImpl}. This map registers all {@link OperatorImpl} in the DAG
   * of {@link OperatorImpl} in a {@link org.apache.samza.container.TaskInstance}. Each {@link OperatorImpl} is created
   * according to a single instance of {@link OperatorSpec}.
   */
  private final Map<OperatorSpec, OperatorImpl> operators = new HashMap<>();

  /**
   * This {@link Map} describes the DAG of {@link OperatorImpl} that are chained together to process the input messages.
   */
  private final Map<SystemStream, RootOperatorImpl> operatorGraph = new HashMap<>();

  private final Clock clock;

  public OperatorGraph(Clock clock) {
    this.clock = clock;
  }

  public OperatorGraph() {
    this(SystemClock.instance());
  }

  /**
   * Initialize the whole DAG of {@link OperatorImpl}s, based on the input {@link MessageStreamImpl} from the {@link org.apache.samza.operators.StreamGraph}.
   * This method will traverse each input {@link org.apache.samza.operators.MessageStream} in the {@code inputStreams} and
   * instantiate the corresponding {@link OperatorImpl} chains that take the {@link org.apache.samza.operators.MessageStream} as input.
   *
   * @param inputStreams  the map of input {@link org.apache.samza.operators.MessageStream}s
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   */
  public void init(Map<SystemStream, MessageStreamImpl> inputStreams, Config config, TaskContext context) {
    inputStreams.forEach((ss, mstream) -> this.operatorGraph.put(ss, this.createOperatorImpls(mstream, config, context)));
  }

  /**
   * Get the {@link RootOperatorImpl} corresponding to the provided {@code ss}.
   *
   * @param ss  input {@link SystemStream}
   * @return  the {@link RootOperatorImpl} that starts processing the input message
   */
  public RootOperatorImpl get(SystemStream ss) {
    return this.operatorGraph.get(ss);
  }

  /**
   * Get all {@link RootOperatorImpl}s for the graph.
   *
   * @return  an immutable view of all {@link RootOperatorImpl}s for the graph
   */
  public Collection<RootOperatorImpl> getAll() {
    return Collections.unmodifiableCollection(this.operatorGraph.values());
  }

  /**
   * Traverses the DAG of {@link OperatorSpec}s starting from the provided {@link MessageStreamImpl},
   * creates the corresponding DAG of {@link OperatorImpl}s, and returns its root {@link RootOperatorImpl} node.
   *
   * @param source  the input {@link MessageStreamImpl} to instantiate {@link OperatorImpl}s for
   * @param <M>  the type of messagess in the {@code source} {@link MessageStreamImpl}
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @return  root node for the {@link OperatorImpl} DAG
   */
  private <M> RootOperatorImpl<M> createOperatorImpls(MessageStreamImpl<M> source, Config config,
      TaskContext context) {
    // since the source message stream might have multiple operator specs registered on it,
    // create a new root node as a single point of entry for the DAG.
    RootOperatorImpl<M> rootOperator = new RootOperatorImpl<>();
    // create the pipeline/topology starting from the source
    source.getRegisteredOperatorSpecs().forEach(registeredOperator -> {
        // pass in the source and context s.t. stateful stream operators can initialize their stores
        OperatorImpl<M, ?> operatorImpl =
            this.createAndRegisterOperatorImpl(registeredOperator, source, config, context);
        rootOperator.registerNextOperator(operatorImpl);
      });
    return rootOperator;
  }

  /**
   * Helper method to recursively traverse the {@link OperatorSpec} DAG and instantiate and link the corresponding
   * {@link OperatorImpl}s.
   *
   * @param operatorSpec  the operatorSpec registered with the {@code source}
   * @param source  the source {@link MessageStreamImpl}
   * @param <M>  type of input message
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @return  the operator implementation for the operatorSpec
   */
  private <M> OperatorImpl<M, ?> createAndRegisterOperatorImpl(OperatorSpec operatorSpec,
      MessageStreamImpl<M> source, Config config, TaskContext context) {
    if (!operators.containsKey(operatorSpec)) {
      OperatorImpl<M, ?> operatorImpl = createOperatorImpl(source, operatorSpec, config, context);
      if (operators.putIfAbsent(operatorSpec, operatorImpl) == null) {
        // this is the first time we've added the operatorImpl corresponding to the operatorSpec,
        // so traverse and initialize and register the rest of the DAG.
        // initialize the corresponding operator function
        operatorSpec.init(config, context);
        MessageStreamImpl nextStream = operatorSpec.getNextStream();
        if (nextStream != null) {
          Collection<OperatorSpec> registeredSpecs = nextStream.getRegisteredOperatorSpecs();
          registeredSpecs.forEach(registeredSpec -> {
              OperatorImpl subImpl = this.createAndRegisterOperatorImpl(registeredSpec, nextStream, config, context);
              operatorImpl.registerNextOperator(subImpl);
            });
        }
        return operatorImpl;
      }
    }

    // the implementation corresponding to operatorSpec has already been instantiated
    // and registered, so we do not need to traverse the DAG further.
    return operators.get(operatorSpec);
  }

  /**
   * Creates a new {@link OperatorImpl} instance for the provided {@link OperatorSpec}.
   *
   * @param source  the source {@link MessageStreamImpl}
   * @param <M>  type of input message
   * @param operatorSpec  the immutable {@link OperatorSpec} definition.
   * @param config  the {@link Config} required to instantiate operators
   * @param context  the {@link TaskContext} required to instantiate operators
   * @return  the {@link OperatorImpl} implementation instance
   */
  private <M> OperatorImpl<M, ?> createOperatorImpl(MessageStreamImpl<M> source, OperatorSpec operatorSpec, Config config, TaskContext context) {
    if (operatorSpec instanceof StreamOperatorSpec) {
      StreamOperatorSpec<M, ?> streamOpSpec = (StreamOperatorSpec<M, ?>) operatorSpec;
      return new StreamOperatorImpl<>(streamOpSpec, source, config, context);
    } else if (operatorSpec instanceof SinkOperatorSpec) {
      return new SinkOperatorImpl<>((SinkOperatorSpec<M>) operatorSpec, config, context);
    } else if (operatorSpec instanceof WindowOperatorSpec) {
      return new WindowOperatorImpl((WindowOperatorSpec<M, ?, ?>) operatorSpec, clock);
    } else if (operatorSpec instanceof PartialJoinOperatorSpec) {
      return new PartialJoinOperatorImpl<>((PartialJoinOperatorSpec) operatorSpec, source, config, context);
    }
    throw new IllegalArgumentException(
        String.format("Unsupported OperatorSpec: %s", operatorSpec.getClass().getName()));
  }
}
