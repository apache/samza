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

package org.apache.samza.sql.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.sql.data.SamzaSqlCompositeKey;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;


public class TableJoinUtils {
  private final Map<String, TableDescriptor> tableDescMap = new HashMap<>();
  private final Serde<SamzaSqlCompositeKey> keySerde = new JsonSerdeV2<>(SamzaSqlCompositeKey.class);
  private final Serde<SamzaSqlRelMessage> valueSerde = new JsonSerdeV2<>(SamzaSqlRelMessage.class);

  public TableDescriptor createDescriptor(String sourceName) {
    TableDescriptor tableDesc = tableDescMap.get(sourceName);

    // Create a table backed by RocksDb store as composite key and relational
    // message as the value.
    if (tableDesc == null) {
      tableDesc = new RocksDbTableDescriptor("JoinTable-" + sourceName)
          .withSerde(KVSerde.of(keySerde, valueSerde));

      tableDescMap.put(sourceName, tableDesc);
    }

    return tableDesc;
  }

  public static Serde<SamzaSqlCompositeKey> getKeySerde() {
    return new JsonSerdeV2<>(SamzaSqlCompositeKey.class);
  }

  public static Serde<SamzaSqlRelMessage> getValueSerde() {
    return new JsonSerdeV2<>(SamzaSqlRelMessage.class);
  }
}
