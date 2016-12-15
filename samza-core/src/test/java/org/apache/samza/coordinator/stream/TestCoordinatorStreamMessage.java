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

package org.apache.samza.coordinator.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.Delete;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.junit.Test;

public class TestCoordinatorStreamMessage {
  @Test
  public void testCoordinatorStreamMessage() {
    CoordinatorStreamMessage message = new CoordinatorStreamMessage("source");
    assertEquals("source", message.getSource());
    assertEquals(CoordinatorStreamMessage.VERSION, message.getVersion());
    assertNotNull(message.getUsername());
    assertTrue(message.getTimestamp() > 0);
    assertTrue(!message.isDelete());
    CoordinatorStreamMessage secondMessage = new CoordinatorStreamMessage(message.getKeyArray(), message.getMessageMap());
    assertEquals(secondMessage, message);
  }

  @Test
  public void testCoordinatorStreamMessageIsDelete() {
    CoordinatorStreamMessage message = new CoordinatorStreamMessage(new Object[] {}, null);
    assertTrue(message.isDelete());
    assertNull(message.getMessageMap());
  }

  @Test
  public void testSetConfig() {
    SetConfig setConfig = new SetConfig("source", "key", "value");
    assertEquals(SetConfig.TYPE, setConfig.getType());
    assertEquals("key", setConfig.getKey());
    assertEquals("value", setConfig.getConfigValue());
    assertFalse(setConfig.isDelete());
    assertEquals(CoordinatorStreamMessage.VERSION, setConfig.getVersion());
  }

  @Test
  public void testDelete() {
    Delete delete = new Delete("source2", "key", "delete-type");
    assertEquals("delete-type", delete.getType());
    assertEquals("key", delete.getKey());
    assertNull(delete.getMessageMap());
    assertTrue(delete.isDelete());
    assertEquals(CoordinatorStreamMessage.VERSION, delete.getVersion());
  }

  @Test
  public void testHashCodeAndEquality() {
    SetConfig message = new SetConfig("source", "key1", "value1");
    SetConfig message1 = new SetConfig("source", "key1", "value1");
    SetConfig message2 = new SetConfig("source", "key2", "value1");

    assertEquals(message.hashCode(), message1.hashCode());
    assertEquals(message, message1);
    assertTrue(!message.equals(message2));
  }
}
