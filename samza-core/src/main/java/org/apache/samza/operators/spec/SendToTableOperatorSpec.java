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

import java.io.IOException;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.WatermarkFunction;
import org.apache.samza.table.TableSpec;


/**
 * The spec for operator that writes a stream to a table by extracting keys and values
 * from the incoming messages.
 *
 * @param <K> the type of the table record key
 * @param <V> the type of the table record value
 */
@InterfaceStability.Unstable
public class SendToTableOperatorSpec<K, V> extends OperatorSpec<KV<K, V>, Void> {

  private final TableSpec tableSpec;

  /**
   * Constructor for a {@link SendToTableOperatorSpec}.
   *
   * @param tableSpec  the table spec of the table written to
   * @param opId  the unique ID for this operator
   */
  SendToTableOperatorSpec(TableSpec tableSpec, String opId) {
    super(OpCode.SEND_TO, opId);
    this.tableSpec = tableSpec;
  }

  public TableSpec getTableSpec() {
    return tableSpec;
  }

  @Override
  public WatermarkFunction getWatermarkFn() {
    return null;
  }

  public SendToTableOperatorSpec<K, V> copy() throws IOException, ClassNotFoundException {
    return (SendToTableOperatorSpec<K, V>) super.copy();
  }
}
