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

import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.operators.functions.JoinFunction;


/**
 * The spec for the join operator that buffers messages from one stream and
 * joins them with buffered messages from another stream.
 *
 * @param <K>  the type of join key
 * @param <M>  the type of message in this stream
 * @param <JM>  the type of message in the other stream
 * @param <RM>  the type of join result
 */
public class JoinOperatorSpec<K, M, JM, RM> extends OperatorSpec<Object, RM> { // Object == M | JM

  private final OperatorSpec<?, M> leftInputOpSpec;
  private final OperatorSpec<?, JM> rightInputOpSpec;
  private final JoinFunction<K, M, JM, RM> joinFn;
  private final long ttlMs;

  /**
   * Default constructor for a {@link JoinOperatorSpec}.
   *
   * @param leftInputOpSpec  the operator spec for the stream on the left side of the join
   * @param rightInputOpSpec  the operator spec for the stream on the right side of the join
   * @param joinFn  the user-defined join function to get join keys and results
   * @param ttlMs  the ttl in ms for retaining messages in each stream
   * @param opId  the unique ID for this operator
   */
  JoinOperatorSpec(OperatorSpec<?, M> leftInputOpSpec, OperatorSpec<?, JM> rightInputOpSpec,
      JoinFunction<K, M, JM, RM> joinFn, long ttlMs, int opId) {
    super(OpCode.JOIN, opId);
    this.leftInputOpSpec = leftInputOpSpec;
    this.rightInputOpSpec = rightInputOpSpec;
    this.joinFn = joinFn;
    this.ttlMs = ttlMs;
  }

  public OperatorSpec getLeftInputOpSpec() {
    return leftInputOpSpec;
  }

  public OperatorSpec getRightInputOpSpec() {
    return rightInputOpSpec;
  }

  public JoinFunction<K, M, JM, RM> getJoinFn() {
    return this.joinFn;
  }

  public long getTtlMs() {
    return ttlMs;
  }

  @Override
  public InitableFunction getTransformFn() {
    return joinFn;
  }
}
