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

import org.apache.samza.operators.api.data.Message;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.HashSet;
import java.util.Set;


/**
 * Abstract base class for all stream operator implementation classes.
 */
public abstract class OperatorImpl<M extends Message, RM extends Message>
    implements Processor<ProcessorContext<M>, ProcessorContext<RM>> {

  private final Set<Subscriber<? super ProcessorContext<RM>>> subscribers = new HashSet<>();

  @Override public void subscribe(Subscriber<? super ProcessorContext<RM>> s) {
    // Only add once
    subscribers.add(s);
  }

  @Override public void onSubscribe(Subscription s) {

  }

  @Override public void onNext(ProcessorContext<M> o) {

    onNext(o.getMessage(), o.getCollector(), o.getCoordinator());
  }

  @Override public void onError(Throwable t) {

  }

  @Override public void onComplete() {

  }

  /**
   * Each sub-class will implement this method to actually perform the transformation and call the downstream subscribers.
   *
   * @param message  the input {@link Message}
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   */
  protected abstract void onNext(M message, MessageCollector collector, TaskCoordinator coordinator);

  /**
   * Stateful operators will need to override this method to initialize the operators
   *
   * @param context  the task context to initialize the operators within
   */
  protected void init(TaskContext context) {};

  /**
   * Method to trigger all downstream operators that consumes the output {@link org.apache.samza.operators.api.MessageStream}
   * from this operator
   *
   * @param omsg  output {@link Message}
   * @param collector  the {@link MessageCollector} in the context
   * @param coordinator  the {@link TaskCoordinator} in the context
   */
  protected void nextProcessors(RM omsg, MessageCollector collector, TaskCoordinator coordinator) {
    subscribers.forEach(sub ->
      sub.onNext(new ProcessorContext<>(omsg, collector, coordinator))
    );
  }
}
