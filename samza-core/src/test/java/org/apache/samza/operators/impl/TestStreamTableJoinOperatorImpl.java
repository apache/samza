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

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.functions.KeyedStreamTableJoinFunction;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.table.ReadableTable;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import junit.framework.Assert;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestStreamTableJoinOperatorImpl {
  @Test
  public void testHandleMessage() {

    String tableId = "t1";
    TableSpec tableSpec = mock(TableSpec.class);
    when(tableSpec.getId()).thenReturn(tableId);

    StreamTableJoinOperatorSpec mockJoinOpSpec = mock(StreamTableJoinOperatorSpec.class);
    when(mockJoinOpSpec.getTableSpec()).thenReturn(tableSpec);
    when(mockJoinOpSpec.getJoinFn()).thenReturn(new KeyedStreamTableJoinFunction<String, String, String, String>() {
      @Override
      public String apply(KV<String, String> message, KV<String, String> record) {
        if ("1".equals(message.getKey())) {
          Assert.assertEquals("m1", message.getValue());
          Assert.assertEquals("r1", record.getValue());
          return "m1r1";
        } else if ("2".equals(message.getKey())) {
          Assert.assertEquals("m2", message.getValue());
          Assert.assertNull(record);
          return null;
        }
        throw new SamzaException("Should never reach here!");
      }
    });
    Config config = mock(Config.class);
    ReadableTable table = mock(ReadableTable.class);
    when(table.get("1")).thenReturn("r1");
    when(table.get("2")).thenReturn(null);
    TaskContext mockTaskContext = mock(TaskContext.class);
    when(mockTaskContext.getTable(tableId)).thenReturn(table);

    MessageCollector mockMessageCollector = mock(MessageCollector.class);
    TaskCoordinator mockTaskCoordinator = mock(TaskCoordinator.class);

    StreamTableJoinOperatorImpl streamTableJoinOperator = new StreamTableJoinOperatorImpl(
        mockJoinOpSpec, config, mockTaskContext);

    // Table has the key
    Collection<TestMessageEnvelope> result;
    result = streamTableJoinOperator.handleMessage(KV.of("1", "m1"), mockMessageCollector, mockTaskCoordinator);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("m1r1", result.iterator().next());
    // Table doesn't have the key
    result = streamTableJoinOperator.handleMessage(KV.of("2", "m2"), mockMessageCollector, mockTaskCoordinator);
    Assert.assertEquals(0, result.size());
  }

}
