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
package org.apache.samza.operators.spec;

import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.task.TaskContext;


/**
 * Spec for the partial join operator that takes messages from one input stream, joins with buffered
 * messages from another stream, and produces join results to an output {@link MessageStreamImpl}.
 *
 * @param <M>  the type of input message
 * @param <K>  the type of join key
 * @param <JM>  the type of message in the other join stream
 * @param <RM>  the type of message in the join output stream
 */
public class PartialJoinOperatorSpec<M, K, JM, RM> implements OperatorSpec<RM> {

  private final MessageStreamImpl<RM> joinOutput;

  /**
   * The transformation function of {@link PartialJoinOperatorSpec} that takes an input message of
   * type {@code M}, joins with a stream of buffered messages of type {@code JM} from another stream,
   * and generates a joined result message of type {@code RM}.
   */
  private final PartialJoinFunction<K, M, JM, RM> transformFn;


  /**
   * The unique ID for this operator.
   */
  private final int opId;

  /**
   * Default constructor for a {@link PartialJoinOperatorSpec}.
   *
   * @param partialJoinFn  partial join function that take type {@code M} of input message and join
   *                       w/ type {@code JM} of buffered message from another stream
   * @param joinOutput  the output {@link MessageStreamImpl} of the join results
   */
  PartialJoinOperatorSpec(PartialJoinFunction<K, M, JM, RM> partialJoinFn, MessageStreamImpl<RM> joinOutput, int opId) {
    this.joinOutput = joinOutput;
    this.transformFn = partialJoinFn;
    this.opId = opId;
  }

  @Override
  public MessageStreamImpl<RM> getNextStream() {
    return this.joinOutput;
  }

  public PartialJoinFunction<K, M, JM, RM> getTransformFn() {
    return this.transformFn;
  }

  public OperatorSpec.OpCode getOpCode() {
    return OpCode.JOIN;
  }

  public int getOpId() {
    return this.opId;
  }

  @Override public void init(Config config, TaskContext context) {
    this.transformFn.init(config, context);
  }
}
