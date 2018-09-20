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

package org.apache.samza.sql.translator;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.storage.kv.RocksDbTableProvider;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableSpec;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;


/**
 * Base class for all unit tests for translators
 */
public class TranslatorTestBase {
  Map<Integer, MessageStream> registeredStreams = new HashMap<>();
  Map<String, TableImpl> registeredTables = new HashMap<>();

  Answer getRegisterMessageStreamAnswer() {
    return (InvocationOnMock x) -> {
      Integer id = x.getArgumentAt(0, Integer.class);
      MessageStream stream = x.getArgumentAt(1, MessageStream.class);
      registeredStreams.put(id, stream);
      return null;
    };
  }

  Answer getRegisteredTableAnswer() {
    return (InvocationOnMock x) -> {
      TableDescriptor descriptor = x.getArgumentAt(0, TableDescriptor.class);
      TableSpec mockTableSpec = new TableSpec(descriptor.getTableId(), KVSerde.of(new StringSerde(),
          new JsonSerdeV2<SamzaSqlRelMessage>()), RocksDbTableProvider.class.getCanonicalName(), new HashMap<>());
      TableImpl mockTable = mock(TableImpl.class);
      when(mockTable.getTableSpec()).thenReturn(mockTableSpec);
      this.registeredTables.putIfAbsent(descriptor.getTableId(), mockTable);
      return this.registeredTables.get(descriptor.getTableId());
    };
  }

  MessageStream getRegisteredMessageStream(int id) {
    return this.registeredStreams.get(id);
  }

}
