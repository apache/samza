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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Collections;


/**
 * Implementation of a stream-table join operator that first retrieve the value of
 * the message key from incoming message, and then apply the join function.
 *
 * @param <K> type of the join key
 * @param <M> type of input messages
 * @param <R> type of the table record
 * @param <JM> type of the join result
 */
class StreamTableJoinOperatorImpl<K, M, R extends KV, JM> extends OperatorImpl<M, JM> {

  private final StreamTableJoinOperatorSpec<K, M, R, JM> joinOpSpec;
  private final ReadWriteUpdateTable<K, ?, ?> table;

  StreamTableJoinOperatorImpl(StreamTableJoinOperatorSpec<K, M, R, JM> joinOpSpec, Context context) {
    this.joinOpSpec = joinOpSpec;
    this.table = context.getTaskContext().getUpdatableTable(joinOpSpec.getTableId());
  }

  @Override
  protected void handleInit(Context context) {
    this.joinOpSpec.getJoinFn().init(context);
  }

  @Override
  protected CompletionStage<Collection<JM>> handleMessageAsync(M message, MessageCollector collector,
      TaskCoordinator coordinator) {
    if (message == null) {
      return CompletableFuture.completedFuture(Collections.emptyList());
    }

    K key = joinOpSpec.getJoinFn().getMessageKey(message);
    Object[] args = joinOpSpec.getArgs();

    return Optional.ofNullable(key)
        .map(joinKey -> table.getAsync(joinKey, args)
            .thenApply(val -> getJoinOutput(joinKey, val, message)))
        .orElseGet(() -> CompletableFuture.completedFuture(getJoinOutput(key, null, message)));
  }

  private Collection<JM> getJoinOutput(K key, Object value, M message) {
    R record = value == null ? null : (R) KV.of(key, value);

    JM output = joinOpSpec.getJoinFn().apply(message, record);

    // The support for inner and outer join will be provided in the jonFn. For inner join, the joinFn might
    // return null, when the corresponding record is absent in the table.
    return output != null ?
        Collections.singletonList(output) : Collections.emptyList();
  }

  @Override
  protected void handleClose() {
    this.joinOpSpec.getJoinFn().close();
  }

  protected OperatorSpec<M, JM> getOperatorSpec() {
    return joinOpSpec;
  }

}
