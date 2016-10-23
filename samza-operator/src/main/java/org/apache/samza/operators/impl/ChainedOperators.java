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
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.internal.OperatorChain;
import org.apache.samza.operators.internal.Operators.Operator;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;


/**
 * Implementation class for a chain of operators from the single input {@code source}
 *
 * @param <M>  type of message in the input stream {@code source}
 */
public class ChainedOperators<M extends Message> implements OperatorChain<M> {

  private final Set<OperatorImpl> subscribers = new HashSet<>();

  /**
   * Private constructor
   *
   * @param source  the input source {@link MessageStream}
   * @param context  the {@link TaskContext} object that we need to instantiate the state stores
   */
  ChainedOperators(MessageStream<M> source, TaskContext context) {
    // create the pipeline/topology starting from source
    source.getSubscribers().forEach(sub -> {
      // pass in the context s.t. stateful stream operators can initialize their stores
      OperatorImpl subImpl = this.createAndSubscribe(sub, source, context);
      this.subscribers.add(subImpl);
    });
  }

  /**
   * Private function to recursively instantiate the implementation of operators and the chains
   *
   * @param operator  the operator that subscribe to {@code source}
   * @param source  the source {@link MessageStream}
   * @param context  the context of the task
   * @return  the implementation object of the corresponding {@code operator}
   */
  private OperatorImpl<M, ? extends Message> createAndSubscribe(Operator operator, MessageStream source,
      TaskContext context) {
    Entry<OperatorImpl<M, ? extends Message>, Boolean> factoryEntry = OperatorFactory.getOperator(operator);
    if (factoryEntry.getValue()) {
      // The operator has already been instantiated and we do not need to traverse and create the subscribers any more.
      return factoryEntry.getKey();
    }
    OperatorImpl<M, ? extends Message> opImpl = factoryEntry.getKey();
    MessageStream outStream = operator.getOutputStream();
    Collection<Operator> subs = outStream.getSubscribers();
    subs.forEach(sub -> {
      OperatorImpl subImpl = this.createAndSubscribe(sub, operator.getOutputStream(), context);
      opImpl.subscribe(subImpl);
    });
    // initialize the operator's state store
    opImpl.init(source, context);
    return opImpl;
  }


  /**
   * Method to navigate the incoming {@code message} through the processing chains
   *
   * @param message  the incoming message to this {@link ChainedOperators}
   * @param collector  the {@link MessageCollector} object within the process context
   * @param coordinator  the {@link TaskCoordinator} object within the process context
   */
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    this.subscribers.forEach(sub -> sub.onNext(message, collector, coordinator));
  }

  /**
   * Method to handle timer events
   *
   * @param collector  the {@link MessageCollector} object within the process context
   * @param coordinator  the {@link TaskCoordinator} object within the process context
   */
  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    long nanoTime = System.nanoTime();
    this.subscribers.forEach(sub -> sub.onTimer(nanoTime, collector, coordinator));
  }
}
