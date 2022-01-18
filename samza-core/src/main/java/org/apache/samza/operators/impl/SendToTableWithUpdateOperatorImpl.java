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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.UpdateOptions;
import org.apache.samza.operators.UpdateMessage;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.spec.SendToTableWithUpdateOperatorSpec;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.RecordNotFoundException;
import org.apache.samza.table.batching.CompactBatchProvider;
import org.apache.samza.table.remote.RemoteTable;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of a send-to-table operator that applies updates to existing records
 * in the table. If there is no pre-existing record, based on Table's implementation it might attempt to write a
 * default record and then applies an update.
 *
 * @param <K> the type of the record key
 * @param <V> the type of the record value
 * @param <U> the type of the update applied to this table
 */
public class SendToTableWithUpdateOperatorImpl<K, V, U>
    extends OperatorImpl<KV<K, UpdateMessage<U, V>>, KV<K, UpdateMessage<U, V>>> {
  private static final Logger LOG = LoggerFactory.getLogger(SendToTableWithUpdateOperatorImpl.class);

  private final SendToTableWithUpdateOperatorSpec<K, V, U> spec;
  private final ReadWriteUpdateTable<K, V, U> table;

  public SendToTableWithUpdateOperatorImpl(SendToTableWithUpdateOperatorSpec<K, V, U> spec, Context context) {
    this.spec = spec;
    this.table = context.getTaskContext().getUpdatableTable(spec.getTableId());
    if (context.getTaskContext().getUpdatableTable(spec.getTableId()) instanceof RemoteTable) {
      RemoteTable<K, V, U> remoteTable = (RemoteTable<K, V, U>) table;
      if (remoteTable.getBatchProvider() instanceof CompactBatchProvider) {
        throw new SamzaException("Batching is not supported with Compact Batches for partial updates");
      }
    }
  }

  @Override
  protected void handleInit(Context context) {
  }

  @Override
  protected CompletionStage<Collection<KV<K, UpdateMessage<U, V>>>> handleMessageAsync(KV<K, UpdateMessage<U, V>> message,
      MessageCollector collector, TaskCoordinator coordinator) {
    final UpdateOptions options = spec.getUpdateOptions();
    final CompletableFuture<Void> updateFuture = table.updateAsync(message.getKey(), message.getValue().getUpdate());

    return updateFuture
        .handle((result, ex) -> {
          if (ex == null) {
            // success, no need to Put a default value
            return false;
          } else if (ex.getCause() instanceof RecordNotFoundException && message.getValue().hasDefault()) {
            // If an update fails for a given key due to a RecordDoesNotExistException exception thrown and
            // a default is provided and the UpdateOptions is set to UPDATE_WITH_DEFAULTS, then attempt
            // to PUT a default record for the key and then apply the update.
            if (options == UpdateOptions.UPDATE_WITH_DEFAULTS) {
              return true;
            } else {
              throw new SamzaException("Put default failed for update as the Update options was set to " + options +
                  ". Please use UpdateOptions.UPDATE_WITH_DEFAULTS instead.");
            }
          } else {
            throw new SamzaException("Update failed with exception: ", ex);
          }
        })
        .thenCompose(shouldPutDefault -> {
          if (shouldPutDefault) {
            final CompletableFuture<Void> putFuture = table.putAsync(message.getKey(), message.getValue().getDefault());
            return putFuture
                .exceptionally(ex -> {
                  LOG.warn("PUT default failed due to an exception. Ignoring the exception and proceeding with update. "
                          + "The exception encountered is: ", ex);
                  return null;
                })
                .thenCompose(res -> table.updateAsync(message.getKey(), message.getValue().getUpdate()))
                .exceptionally(ex -> {
                  throw new SamzaException("Update after Put default failed with exception: ", ex);
                });
          } else {
            return CompletableFuture.completedFuture(null);
          }
        }).thenApply(result -> Collections.singleton(message));
  }

  @Override
  protected void handleClose() {
    table.close();
  }

  @Override
  protected OperatorSpec<KV<K, UpdateMessage<U, V>>, KV<K, UpdateMessage<U, V>>> getOperatorSpec() {
    return spec;
  }
}
