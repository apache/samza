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
import java.util.concurrent.CompletionStage;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.UpdatePair;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.SendUpdateToTableOperatorSpec;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;

/**
 * Implementation of a send-update-stream-to-table operator that applies updates to existing records
 * in the table. If there is no pre-existing record, based on Table's implementation it attempts to write a a new record.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @param <U> the type of the update applied to this table
 */
public class SendUpdateToTableOperatorImpl<K, V, U> extends OperatorImpl<KV<K, UpdatePair<U, V>>, KV<K, UpdatePair<U, V>>> {
  private final SendUpdateToTableOperatorSpec<K, V, U> sendUpdateToTableOpSpec;
  private final ReadWriteTable<K, V, U> table;

  public SendUpdateToTableOperatorImpl(SendUpdateToTableOperatorSpec<K, V, U>  sendUpdateToTableOpSpec, Context context) {
    this.sendUpdateToTableOpSpec = sendUpdateToTableOpSpec;
    this.table = context.getTaskContext().getTable(sendUpdateToTableOpSpec.getTableId());
  }

  @Override
  protected void handleInit(Context context) {
  }

  @Override
  protected CompletionStage<Collection<KV<K, UpdatePair<U, V>>>> handleMessageAsync(KV<K, UpdatePair<U, V>> message,
      MessageCollector collector, TaskCoordinator coordinator) {
    return table.updateAsync(message.getKey(), message.getValue().getUpdate(), message.getValue().getDefault(),
        sendUpdateToTableOpSpec.getArgs())
        .thenApply(result -> Collections.singleton(message));
  }

  @Override
  protected void handleClose() {
    table.close();
  }

  @Override
  protected OperatorSpec<KV<K, UpdatePair<U, V>>, KV<K, UpdatePair<U, V>>> getOperatorSpec() {
    return sendUpdateToTableOpSpec;
  }
}
