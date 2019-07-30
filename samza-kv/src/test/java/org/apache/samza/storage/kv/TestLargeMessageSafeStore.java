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
package org.apache.samza.storage.kv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.LongSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class TestLargeMessageSafeStore {

  @Mock
  KeyValueStore<byte[], byte[]> store;
  int maxMessageSize = 1024;
  String storeName = "testStore";

  @Before
  public void setup() {
    Mockito.doNothing().when(store).put(Matchers.any(), Matchers.any());
    Mockito.doNothing().when(store).putAll(Matchers.any());
  }

  @Test
  public void testSmallMessagePut() {
    LargeMessageSafeStore largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize);

    byte[] key = new byte[16];
    byte[] smallMessage = new byte[32];
    largeMessageSafeKeyValueStore.put(key, smallMessage);
    Mockito.verify(store).put(Matchers.eq(key), Matchers.eq(smallMessage));
  }

  @Test
  public void testLargeMessagePutWithDropLargeMessageDisabled() {
    LargeMessageSafeStore largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize);

    byte[] key = new byte[16];
    byte[] largeMessage = new byte[maxMessageSize + 1];
    try {
      largeMessageSafeKeyValueStore.put(key, largeMessage);
      Assert.fail("The test case should have failed due to a large message being passed to the changelog, but it didn't.");
    } catch (RecordTooLargeException e) {
      Mockito.verifyZeroInteractions(store);
    }
  }

  @Test
  public void testSmallMessagePutAllSuccessWithDropLargeMessageDisabled() {
    LargeMessageSafeStore largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize);
    byte[] key = new byte[16];
    byte[] smallMessage = new byte[32];

    List<Entry<byte[], byte[]>> entries = new ArrayList<>();
    entries.add(new Entry<>(key, smallMessage));

    largeMessageSafeKeyValueStore.putAll(entries);
    Mockito.verify(store).putAll(Matchers.eq(entries));
    Mockito.verify(store, Mockito.never()).put(Matchers.any(byte[].class), Matchers.any(byte[].class));
  }

  @Test
  public void testLargeMessagePutAllFailureWithDropLargeMessageDisabled() {
    LargeMessageSafeStore largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize);
    byte[] key = new byte[16];
    byte[] largeMessage = new byte[maxMessageSize + 1];

    List<Entry<byte[], byte[]>> entries = new ArrayList<>();
    entries.add(new Entry<>(key, largeMessage));

    try {
      largeMessageSafeKeyValueStore.putAll(entries);
      Assert.fail("The test case should have failed due to a large message being passed to the changelog, but it didn't.");
    } catch (RecordTooLargeException e) {
      Mockito.verifyZeroInteractions(store);
    }
  }

  @Test
  public void testSmallMessagePutWithSerdeAndDropLargeMessageDisabled() {
    LargeMessageSafeStore largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize);

    Serde<Long> longSerde = new LongSerde();
    long longObj = 1000L;
    byte[] key = longSerde.toBytes(longObj);

    JsonSerdeV2<Map<String, Object>> jsonSerde = new JsonSerdeV2<>();
    Map<String, Object> obj = new HashMap<>();
    obj.put("jack", "jill");
    obj.put("john", 2);
    byte[] smallMessage = jsonSerde.toBytes(obj);

    List<Entry<byte[], byte[]>> entries = new ArrayList<>();
    entries.add(new Entry<>(key, smallMessage));

    largeMessageSafeKeyValueStore.putAll(entries);
    Mockito.verify(store).putAll(Matchers.eq(entries));
    Mockito.verify(store, Mockito.never()).put(Matchers.any(byte[].class), Matchers.any(byte[].class));
  }

  @Test
  public void testLargeMessagePutWithSerdeAndDropLargeMessageDisabled() {
    LargeMessageSafeStore largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, false, maxMessageSize);

    Serde<Long> longSerde = new LongSerde();
    long longObj = 1000L;
    byte[] key = longSerde.toBytes(longObj);

    Serde<String> stringSerde = new StringSerde();
    String largeString = StringUtils.repeat("a", maxMessageSize + 1);
    byte[] largeMessage = stringSerde.toBytes(largeString);

    List<Entry<byte[], byte[]>> entries = new ArrayList<>();
    entries.add(new Entry<>(key, largeMessage));

    try {
      largeMessageSafeKeyValueStore.putAll(entries);
    } catch (RecordTooLargeException e) {
      Mockito.verifyZeroInteractions(store);
    }
  }

  @Test
  public void testSmallMessagePutSuccessWithDropLargeMessageEnabled() {
    LargeMessageSafeStore largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, true, maxMessageSize);
    byte[] key = new byte[16];
    byte[] smallMessage = new byte[32];

    largeMessageSafeKeyValueStore.put(key, smallMessage);

    Mockito.verify(store).put(Matchers.eq(key), Matchers.eq(smallMessage));
  }

  @Test
  public void testLargeMessagePutSuccessWithDropLargeMessageEnabled() {
    LargeMessageSafeStore largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, true, maxMessageSize);
    byte[] key = new byte[16];
    byte[] largeMessage = new byte[maxMessageSize + 1];

    largeMessageSafeKeyValueStore.put(key, largeMessage);

    Mockito.verifyZeroInteractions(store);
  }

  @Test
  public void testPutAllSuccessWithDropLargeMessageEnabled() {
    LargeMessageSafeStore largeMessageSafeKeyValueStore = new LargeMessageSafeStore(store, storeName, true, maxMessageSize);
    byte[] key1 = new byte[16];
    byte[] largeMessage = new byte[maxMessageSize + 1];
    byte[] key2 = new byte[8];
    byte[] smallMessage = new byte[1];

    List<Entry<byte[], byte[]>> entries = new ArrayList<>();
    Entry<byte[], byte[]> largeMessageEntry = new Entry<>(key1, largeMessage);
    Entry<byte[], byte[]> smallMessageEntry = new Entry<>(key2, smallMessage);
    entries.add(largeMessageEntry);
    entries.add(smallMessageEntry);

    largeMessageSafeKeyValueStore.putAll(entries);

    entries.remove(largeMessageEntry);
    Mockito.verify(store).putAll(Matchers.eq(entries));
    Mockito.verify(store, Mockito.never()).put(Matchers.any(byte[].class), Matchers.any(byte[].class));
  }
}
