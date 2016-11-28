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
package org.apache.samza.operators.data;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestIncomingSystemMessage {

  @Test
  public void testConstructor() {
    IncomingMessageEnvelope ime = mock(IncomingMessageEnvelope.class);
    IncomingSystemMessage ism = new IncomingSystemMessage(ime);

    Object mockKey = mock(Object.class);
    Object mockValue = mock(Object.class);
    LongOffset testOffset = new LongOffset("12345");
    SystemStreamPartition mockSsp = mock(SystemStreamPartition.class);

    when(ime.getKey()).thenReturn(mockKey);
    when(ime.getMessage()).thenReturn(mockValue);
    when(ime.getSystemStreamPartition()).thenReturn(mockSsp);
    when(ime.getOffset()).thenReturn("12345");

    assertEquals(ism.getKey(), mockKey);
    assertEquals(ism.getMessage(), mockValue);
    assertEquals(ism.getSystemStreamPartition(), mockSsp);
    assertEquals(ism.getOffset(), testOffset);
    assertFalse(ism.isDelete());
  }
}
