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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import junit.framework.Assert;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.context.MockContext;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.data.TestMessageEnvelope;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.operators.spec.StreamTableJoinOperatorSpec;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class TestStreamTableJoinOperatorImpl {
  @Test
  public void testHandleMessage() {

    String tableId = "t1";

    StreamTableJoinOperatorSpec mockJoinOpSpec = mock(StreamTableJoinOperatorSpec.class);
    when(mockJoinOpSpec.getTableId()).thenReturn(tableId);
    when(mockJoinOpSpec.getArgs()).thenReturn(new Object[0]);
    when(mockJoinOpSpec.getJoinFn()).thenReturn(
        new StreamTableJoinFunction<String, KV<String, String>, KV<String, String>, String>() {
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

          @Override
          public String getMessageKey(KV<String, String> message) {
            return message.getKey();
          }

          @Override
          public String getRecordKey(KV<String, String> record) {
            return record.getKey();
          }
        });
    ReadWriteUpdateTable table = mock(ReadWriteUpdateTable.class);
    when(table.getAsync("1")).thenReturn(CompletableFuture.completedFuture("r1"));
    when(table.getAsync("2")).thenReturn(CompletableFuture.completedFuture(null));
    Context context = new MockContext();
    when(context.getTaskContext().getUpdatableTable(tableId)).thenReturn(table);

    MessageCollector mockMessageCollector = mock(MessageCollector.class);
    TaskCoordinator mockTaskCoordinator = mock(TaskCoordinator.class);

    StreamTableJoinOperatorImpl streamTableJoinOperator = new StreamTableJoinOperatorImpl(mockJoinOpSpec, context);

    // Table has the key
    Collection<TestMessageEnvelope> result;
    result = streamTableJoinOperator.handleMessage(KV.of("1", "m1"), mockMessageCollector, mockTaskCoordinator);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("m1r1", result.iterator().next());
    // Table doesn't have the key
    result = streamTableJoinOperator.handleMessage(KV.of("2", "m2"), mockMessageCollector, mockTaskCoordinator);
    Assert.assertEquals(0, result.size());
  }

  /**
   * Ensure join function is not invoked more than once when join function returns null on the first invocation
   */
  @Test
  public void testJoinFunctionIsInvokedOnlyOnce() {
    final String tableId = "testTable";
    final CountDownLatch joinInvokedLatch = new CountDownLatch(1);

    StreamTableJoinOperatorSpec mockJoinOpSpec = mock(StreamTableJoinOperatorSpec.class);
    when(mockJoinOpSpec.getTableId()).thenReturn(tableId);
    when(mockJoinOpSpec.getArgs()).thenReturn(new Object[0]);
    when(mockJoinOpSpec.getJoinFn()).thenReturn(
        new StreamTableJoinFunction<String, KV<String, String>, KV<String, String>, String>() {
          @Override
          public String apply(KV<String, String> message, KV<String, String> record) {
            joinInvokedLatch.countDown();
            return null;
          }

          @Override
          public String getMessageKey(KV<String, String> message) {
            return message.getKey();
          }

          @Override
          public String getRecordKey(KV<String, String> record) {
            return record.getKey();
          }
        });

    ReadWriteUpdateTable table = mock(ReadWriteUpdateTable.class);
    when(table.getAsync("1")).thenReturn(CompletableFuture.completedFuture("r1"));

    Context context = new MockContext();
    when(context.getTaskContext().getUpdatableTable(tableId)).thenReturn(table);

    MessageCollector mockMessageCollector = mock(MessageCollector.class);
    TaskCoordinator mockTaskCoordinator = mock(TaskCoordinator.class);

    StreamTableJoinOperatorImpl streamTableJoinOperator = new StreamTableJoinOperatorImpl(mockJoinOpSpec, context);

    // Table has the key
    streamTableJoinOperator.handleMessage(KV.of("1", "m1"), mockMessageCollector, mockTaskCoordinator);
    assertEquals("Join function should only be invoked once", 0, joinInvokedLatch.getCount());
  }

}
