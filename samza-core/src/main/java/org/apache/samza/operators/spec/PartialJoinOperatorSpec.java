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

import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.functions.PartialJoinFunction;
import org.apache.samza.operators.util.OperatorJsonUtils;


/**
 * Spec for the partial join operator that takes messages from one input stream, joins with buffered
 * messages from another stream, and produces join results to an output {@link MessageStreamImpl}.
 *
 * @param <K>  the type of join key
 * @param <M>  the type of input message
 * @param <JM>  the type of message in the other join stream
 * @param <RM>  the type of message in the join output stream
 */
public class PartialJoinOperatorSpec<K, M, JM, RM> implements OperatorSpec<RM> {

  private final PartialJoinFunction<K, M, JM, RM> thisPartialJoinFn;
  private final PartialJoinFunction<K, JM, M, RM> otherPartialJoinFn;
  private final long ttlMs;
  private final MessageStreamImpl<RM> nextStream;
  private final int opId;
  private final String sourceLocation;

  /**
   * Default constructor for a {@link PartialJoinOperatorSpec}.
   *
   * @param thisPartialJoinFn  partial join function that provides state and the join logic for input messages of
   *                           type {@code M} in this stream
   * @param otherPartialJoinFn  partial join function that provides state for input messages of type {@code JM}
   *                            in the other stream
   * @param ttlMs  the ttl in ms for retaining messages in each stream
   * @param nextStream  the output {@link MessageStreamImpl} containing the messages produced from this operator
   * @param opId  the unique ID for this operator
   */
  PartialJoinOperatorSpec(PartialJoinFunction<K, M, JM, RM> thisPartialJoinFn,
      PartialJoinFunction<K, JM, M, RM> otherPartialJoinFn, long ttlMs,
      MessageStreamImpl<RM> nextStream, int opId) {
    this.thisPartialJoinFn = thisPartialJoinFn;
    this.otherPartialJoinFn = otherPartialJoinFn;
    this.ttlMs = ttlMs;
    this.nextStream = nextStream;
    this.opId = opId;
    this.sourceLocation = OperatorJsonUtils.getSourceLocation();
  }

  @Override
  public MessageStreamImpl<RM> getNextStream() {
    return this.nextStream;
  }

  public PartialJoinFunction<K, M, JM, RM> getThisPartialJoinFn() {
    return this.thisPartialJoinFn;
  }

  public PartialJoinFunction<K, JM, M, RM> getOtherPartialJoinFn() {
    return this.otherPartialJoinFn;
  }

  public long getTtlMs() {
    return ttlMs;
  }

  @Override
  public OperatorSpec.OpCode getOpCode() {
    return OpCode.JOIN;
  }

  @Override
  public int getOpId() {
    return this.opId;
  }

  @Override
  public String getSourceLocation() {
    return sourceLocation;
  }
}
