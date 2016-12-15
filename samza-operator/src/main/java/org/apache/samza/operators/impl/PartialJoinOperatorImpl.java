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

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.spec.PartialJoinOperatorSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;


/**
 * Implementation of a {@link PartialJoinOperatorSpec}. This class implements function
 * that only takes in one input stream among all inputs to the join and generate the join output.
 *
 * @param <M>  type of {@link MessageEnvelope}s in the input stream
 * @param <JM>  type of {@link MessageEnvelope}s in the stream to join with
 * @param <RM>  type of {@link MessageEnvelope}s in the joined stream
 */
class PartialJoinOperatorImpl<M extends MessageEnvelope<K, ?>, K, JM extends MessageEnvelope<K, ?>, RM extends MessageEnvelope>
    extends OperatorImpl<M, RM> {

  PartialJoinOperatorImpl(PartialJoinOperatorSpec<M, K, JM, RM> joinOp) {
    // TODO: implement PartialJoinOperatorImpl constructor
  }

  @Override
  public void onNext(M message, MessageCollector collector, TaskCoordinator coordinator) {
    // TODO: implement PartialJoinOperatorImpl processing logic
  }
}
