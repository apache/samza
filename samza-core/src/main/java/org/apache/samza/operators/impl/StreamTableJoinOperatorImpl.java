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
 * @param <MV> type of input messages
 * @param <RV> type of the table record value
 * @param <JM> type of the join result
 */
class StreamTableJoinOperatorImpl<K, MV, RV, JM> extends OperatorImpl<KV<K, MV>, JM> {

  private final StreamTableJoinOperatorSpec<K, KV<K, MV>, KV<K, RV>, JM> joinOpSpec;
  private final ReadableTable<K, RV> table;

  StreamTableJoinOperatorImpl(StreamTableJoinOperatorSpec<K, KV<K, MV>, KV<K, RV>, JM> joinOpSpec,
      Config config, TaskContext context) {
    this.joinOpSpec = joinOpSpec;
    this.table = (ReadableTable) context.getTable(joinOpSpec.getTableSpec().getId());
  }

  @Override
  protected void handleInit(Config config, TaskContext context) {
    this.joinOpSpec.getJoinFn().init(config, context);
  }

  @Override
  public Collection<JM> handleMessage(KV<K, MV> message, MessageCollector collector, TaskCoordinator coordinator) {
    K key = message.getKey();
    RV recordValue = table.get(key);
    KV<K, RV> record = recordValue != null ? KV.of(key, recordValue) : null;
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

  protected OperatorSpec<KV<K, MV>, JM> getOperatorSpec() {
    return joinOpSpec;
  }

}
