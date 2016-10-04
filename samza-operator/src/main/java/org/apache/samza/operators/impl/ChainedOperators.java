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

import org.apache.samza.operators.api.MessageStream;
import org.apache.samza.operators.api.data.Message;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


/**
 * Implementation class for a chain of operators from the single input {@code source}
 *
 * @param <M>  type of message in the input stream {@code source}
 */
public class ChainedOperators<M extends Message> {

  /**
   * Private constructor
   *
   * @param source  the input source {@link MessageStream}
   * @param context  the {@link TaskContext} object that we need to instantiate the state stores
   */
  private ChainedOperators(MessageStream<M> source, TaskContext context) {
    // create the pipeline/topology starting from source
    // pass in the context s.t. stateful stream operators can initialize their stores
  }

  /**
   * Static method to create a {@link ChainedOperators} from the {@code source} stream
   *
   * @param source  the input source {@link MessageStream}
   * @param context  the {@link TaskContext} object used to initialize the {@link StateStoreImpl}
   * @param <M>  the type of input {@link Message}
   * @return a {@link ChainedOperators} object takes the {@code source} as input
   */
  public static <M extends Message> ChainedOperators create(MessageStream<M> source, TaskContext context) {
    return new ChainedOperators<>(source, context);
  }

  /**
   * Method to navigate the incoming {@code message} through the processing chains
   *
   * @param message  the incoming message to this {@link ChainedOperators}
   * @param collector  the {@link MessageCollector} object within the process context
   * @param coordinator  the {@link TaskCoordinator} object within the process context
   */
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    // TODO: add implementation of onNext() that actually triggers the process pipeline
  }

  public void onTimer(MessageCollector collector, TaskCoordinator coordinator) {
    // TODO: add implementation of onTimer() that actually calls the corresponding window operator's onTimer() methods
  }
}
