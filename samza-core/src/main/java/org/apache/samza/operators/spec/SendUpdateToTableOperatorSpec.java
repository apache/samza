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

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.UpdateMessage;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.operators.functions.WatermarkFunction;

/**
 * The spec for operator that writes an update stream to a table by extracting keys, updates and defaults
 * from the incoming messages.
 *
 * @param <K> the type of the table record key
 * @param <V> the type of the table record value
 * @param <U> the type of the update
 */
@InterfaceStability.Unstable
public class SendUpdateToTableOperatorSpec<K, V, U> extends OperatorSpec<KV<K, UpdateMessage<U, V>>, KV<K, UpdateMessage<U, V>>> {
  private final String tableId;
  private final Object[] args;

  /**
   * Constructor for a {@link SendToTableOperatorSpec}.
   *
   * @param tableId  the Id of the table written to
   * @param opId  the unique ID for this operator
   * @param args additional arguments passed to the table
   */
  SendUpdateToTableOperatorSpec(String tableId, String opId, Object ... args) {
    super(OpCode.SEND_UPDATE_TO, opId);
    this.tableId = tableId;
    this.args = args;
  }

  public String getTableId() {
    return tableId;
  }

  public Object[] getArgs() {
    return args;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return null;
  }

  @Override
  public ScheduledFunction getScheduledFn() {
    return null;
  }
}
