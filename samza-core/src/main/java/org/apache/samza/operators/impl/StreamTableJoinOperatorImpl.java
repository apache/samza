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

import java.util.Collection;
import java.util.Collections;

import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.OpContext;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;


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
  private final ReadableTable<K, ?> table;

  StreamTableJoinOperatorImpl(StreamTableJoinOperatorSpec<K, M, R, JM> joinOpSpec,
      Config config, TaskContext context) {
    this.joinOpSpec = joinOpSpec;
    this.table = (ReadableTable) context.getTable(joinOpSpec.getTableSpec().getId());
  }

  @Override
  protected void handleInit(Config config, OpContext opContext) {
    this.joinOpSpec.getJoinFn().init(config, opContext);
  }

  @Override
  public Collection<JM> handleMessage(M message, MessageCollector collector, TaskCoordinator coordinator) {
    K key = joinOpSpec.getJoinFn().getMessageKey(message);
    Object recordValue = table.get(key);
    R record = recordValue != null ? (R) KV.of(key, recordValue) : null;
    JM output = joinOpSpec.getJoinFn().apply(message, record);

    // The support for inner and outer join will be provided in the jonFn. For inner join, the joinFn might
    // return null, when the corresponding record is absent in the table.
    return output != null ?
        Collections.singletonList(output)
      : Collections.emptyList();
  }

  @Override
  protected void handleClose() {
    this.joinOpSpec.getJoinFn().close();
  }

  protected OperatorSpec<M, JM> getOperatorSpec() {
    return joinOpSpec;
  }

}
