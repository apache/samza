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


import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.operators.spec.SinkOperatorSpec;
import org.apache.samza.operators.spec.StreamOperatorSpec;
import org.apache.samza.operators.spec.WindowOperatorSpec;
import org.apache.samza.operators.windows.WindowOutput;
import org.apache.samza.operators.windows.WindowState;
import org.apache.samza.task.TaskContext;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Instantiates the DAG of {@link OperatorImpl} implementations corresponding to the {@link OperatorSpec}s.
 */
public class OperatorImpls {

  /**
   * Holds the mapping between the {@link OperatorSpec} and {@link OperatorImpl}s instances.
   */
  private static final Map<OperatorSpec, OperatorImpl> OPERATOR_IMPLS = new ConcurrentHashMap<>();

  /**
   * Traverses the DAG of {@link OperatorSpec} starting from the provided {@link MessageStreamImpl}, creates the
   * corresponding DAG of {@link OperatorImpl}s, and returns its root {@link NoOpOperatorImpl} node.
   *
   * @param source  the input {@link MessageStreamImpl} to instantiate {@link OperatorImpl}s for
   * @param <M>  type of the {@link Message} in the input {@link MessageStreamImpl}
   * @param context  the {@link TaskContext} required to instantiate operators
   * @return  root node for the {@link OperatorImpl} DAG.
   */
  public static <M extends Message> NoOpOperatorImpl<M> createOperatorImpls(MessageStreamImpl<M> source, TaskContext context) {
    // since the source message stream might have multiple operator specs registered on it,
    // create a new root node as a single point of entry for the DAG.
    NoOpOperatorImpl<M> rootOperator = new NoOpOperatorImpl<>();
    // create the pipeline/topology starting from the source
    source.getRegisteredOperatorSpecs().forEach(registeredOperator -> {
        // pass in the source and context s.t. stateful stream operators can initialize their stores
        OperatorImpl<M, ? extends Message> operatorImpl =
            createAndRegisterOperatorImpl(registeredOperator, source, context);
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
   * @param context  the context of the task
   * @return  the operator implementation for the operatorSpec
   */
  private static <M extends Message> OperatorImpl<M, ? extends Message> createAndRegisterOperatorImpl(OperatorSpec operatorSpec,
      MessageStream source, TaskContext context) {
    if (!OPERATOR_IMPLS.containsKey(operatorSpec)) {
      OperatorImpl<M, ? extends Message> operatorImpl = createOperatorImpl(operatorSpec);
      if (OPERATOR_IMPLS.putIfAbsent(operatorSpec, operatorImpl) == null) {
        // this is the first time we've added the operatorImpl corresponding to the operatorSpec,
        // so traverse and initialize and register the rest of the DAG.
        MessageStream<? extends Message> outStream = operatorSpec.getOutputStream();
        Collection<OperatorSpec> registeredSpecs = ((MessageStreamImpl) outStream).getRegisteredOperatorSpecs();
        registeredSpecs.forEach(registeredSpec -> {
            OperatorImpl subImpl = createAndRegisterOperatorImpl(registeredSpec, outStream, context);
            operatorImpl.registerNextOperator(subImpl);
          });
        operatorImpl.init(source, context);
        return operatorImpl;
      }
    }

    // the implementation corresponding to operatorSpec has already been instantiated
    // and registered, so we do not need to traverse the DAG further.
    return OPERATOR_IMPLS.get(operatorSpec);
  }

  /**
   * Creates a new {@link OperatorImpl} instance for the provided {@link OperatorSpec}.
   *
   * @param operatorSpec  the immutable {@link OperatorSpec} definition.
   * @param <M>  type of input {@link Message}
   * @return  the {@link OperatorImpl} implementation instance
   */
  protected static <M extends Message> OperatorImpl<M, ? extends Message> createOperatorImpl(OperatorSpec operatorSpec) {
    if (operatorSpec instanceof StreamOperatorSpec) {
      return new StreamOperatorImpl<>((StreamOperatorSpec<M, ? extends Message>) operatorSpec);
    } else if (operatorSpec instanceof SinkOperatorSpec) {
      return new SinkOperatorImpl<>((SinkOperatorSpec<M>) operatorSpec);
    } else if (operatorSpec instanceof WindowOperatorSpec) {
      return new SessionWindowOperatorImpl<>((WindowOperatorSpec<M, ?, ? extends WindowState, ? extends WindowOutput>) operatorSpec);
    } else if (operatorSpec instanceof PartialJoinOperatorSpec) {
      return new PartialJoinOperatorImpl<>((PartialJoinOperatorSpec) operatorSpec);
    }
    throw new IllegalArgumentException(
        String.format("Unsupported OperatorSpec: %s", operatorSpec.getClass().getName()));
  }
}
