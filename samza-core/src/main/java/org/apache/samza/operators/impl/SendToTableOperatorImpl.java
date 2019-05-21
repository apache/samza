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

import java.util.Collections;
import java.util.concurrent.CompletionStage;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.SendToTableOperatorSpec;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import java.util.Collection;


/**
 * Implementation of a send-stream-to-table operator that stores the record
 * in the table.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 */
public class SendToTableOperatorImpl<K, V> extends OperatorImpl<KV<K, V>, KV<K, V>> {

  private final SendToTableOperatorSpec<K, V> sendToTableOpSpec;
  private final ReadWriteTable<K, V> table;

  SendToTableOperatorImpl(SendToTableOperatorSpec<K, V> sendToTableOpSpec, Context context) {
    this.sendToTableOpSpec = sendToTableOpSpec;
    this.table = context.getTaskContext().getTable(sendToTableOpSpec.getTableId());
  }

  @Override
  protected void handleInit(Context context) {
  }

  @Override
  protected CompletionStage<Collection<KV<K, V>>> handleMessageAsync(KV<K, V> message, MessageCollector collector,
      TaskCoordinator coordinator) {
    return table.putAsync(message.getKey(), message.getValue(), sendToTableOpSpec.getArgs())
        .thenApply(result -> Collections.singleton(message));
  }

  @Override
  protected void handleClose() {
    table.close();
  }

  @Override
  protected OperatorSpec<KV<K, V>, KV<K, V>> getOperatorSpec() {
    return sendToTableOpSpec;
  }
}
