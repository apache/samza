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

import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.windows.StoreFunctions;

import java.util.function.BiFunction;


/**
 * Spec for the partial join operator that takes {@link Message}s from one input stream, joins with buffered
 * {@link Message}s from another stream, and produces join results to an output {@link MessageStreamImpl}.
 *
 * @param <M>  the type of input {@link Message}
 * @param <K>  the type of join key
 * @param <JM>  the type of {@link Message} in the other join stream
 * @param <RM>  the type of {@link Message} in the join output stream
 */
public class PartialJoinOperatorSpec<M extends Message<K, ?>, K, JM extends Message<K, ?>, RM extends Message>
    implements OperatorSpec<RM> {

  private final MessageStreamImpl<RM> joinOutput;

  /**
   * The transformation function of {@link PartialJoinOperatorSpec} that takes an input message of type {@code M},
   * joins with a stream of buffered {@link Message}s of type {@code JM} from another stream, and generates a joined
   * result {@link Message} of type {@code RM}.
   */
  private final BiFunction<M, JM, RM> transformFn;

  /**
   * The message store functions that read the buffered messages from the other stream in the join.
   */
  private final StoreFunctions<JM, K, JM> joinStoreFns;

  /**
   * The message store functions that save the buffered messages of this {@link MessageStreamImpl} in the join.
   */
  private final StoreFunctions<M, K, M> selfStoreFns;

  /**
   * The unique ID for this operator.
   */
  private final String operatorId;

  /**
   * Default constructor for a {@link PartialJoinOperatorSpec}.
   *
   * @param partialJoinFn  partial join function that take type {@code M} of input {@link Message} and join w/ type
   *                     {@code JM} of buffered {@link Message} from another stream
   * @param joinOutput  the output {@link MessageStreamImpl} of the join results
   */
  PartialJoinOperatorSpec(BiFunction<M, JM, RM> partialJoinFn, MessageStreamImpl<RM> joinOutput, String operatorId) {
    this.joinOutput = joinOutput;
    this.transformFn = partialJoinFn;
    // Read-only join store, no creator/updater functions required.
    this.joinStoreFns = new StoreFunctions<>(m -> m.getKey(), null);
    // Buffered message store for this input stream.
    this.selfStoreFns = new StoreFunctions<>(m -> m.getKey(), (m, s1) -> m);
    this.operatorId = operatorId;
  }

  @Override
  public String toString() {
    return this.operatorId;
  }

  @Override
  public MessageStreamImpl<RM> getOutputStream() {
    return this.joinOutput;
  }

  public StoreFunctions<JM, K, JM> getJoinStoreFns() {
    return this.joinStoreFns;
  }

  public StoreFunctions<M, K, M> getSelfStoreFns() {
    return this.selfStoreFns;
  }

  public BiFunction<M, JM, RM> getTransformFn() {
    return this.transformFn;
  }
}
