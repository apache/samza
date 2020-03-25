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
package org.apache.samza.coordinator.metadatastore;

import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestNamespaceAwareCoordinatorStreamStore {
  private static final String KEY1 = "testKey";

  private CoordinatorStreamStore coordinatorStreamStore;
  private NamespaceAwareCoordinatorStreamStore namespaceAwareCoordinatorStreamStore;
  private String namespace;

  @Before
  public void setUp() {
    namespace = RandomStringUtils.randomAlphabetic(5);
    coordinatorStreamStore = Mockito.mock(CoordinatorStreamStore.class);
    namespaceAwareCoordinatorStreamStore = new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, namespace);
  }

  @Test
  public void testGetShouldDelegateTheInvocationToUnderlyingStore() {
    String value = RandomStringUtils.randomAlphabetic(5);
    String namespacedKey = CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, KEY1);
    Mockito.when(coordinatorStreamStore.all()).thenReturn(ImmutableMap.of(namespacedKey, value.getBytes(StandardCharsets.UTF_8)));

    Assert.assertArrayEquals(value.getBytes(StandardCharsets.UTF_8), namespaceAwareCoordinatorStreamStore.get(KEY1));
    Mockito.verify(coordinatorStreamStore).all();
  }

  @Test
  public void testPutShouldDelegateTheInvocationToUnderlyingStore() {
    String value = RandomStringUtils.randomAlphabetic(5);
    String namespacedKey = CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, KEY1);

    Mockito.doNothing().when(coordinatorStreamStore).put(namespacedKey, value.getBytes(StandardCharsets.UTF_8));

    namespaceAwareCoordinatorStreamStore.put(KEY1, value.getBytes(StandardCharsets.UTF_8));

    Mockito.verify(coordinatorStreamStore).put(namespacedKey, value.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testPutAllShouldDelegateTheInvocationToUnderlyingStore() {
    byte[] value = RandomStringUtils.randomAlphabetic(5).getBytes(StandardCharsets.UTF_8);
    ImmutableMap<String, byte[]> map = ImmutableMap.of(
        "key1", value,
        "key2", value,
        "key3", value,
        "key4", value,
        "key5", value);
    ImmutableMap<String, byte[]> namespacedMap = ImmutableMap.of(
        CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, "key1"), value,
        CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, "key2"), value,
        CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, "key3"), value,
        CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, "key4"), value,
        CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, "key5"), value);

    Mockito.doNothing().when(coordinatorStreamStore).putAll(namespacedMap);

    namespaceAwareCoordinatorStreamStore.putAll(map);

    Mockito.verify(coordinatorStreamStore).putAll(namespacedMap);
  }

  @Test
  public void testDeleteShouldDelegateTheInvocationToUnderlyingStore() {
    String namespacedKey = CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, KEY1);

    namespaceAwareCoordinatorStreamStore.delete(KEY1);

    Mockito.verify(coordinatorStreamStore).delete(namespacedKey);
  }

  @Test
  public void testAllShouldDelegateToUnderlyingMetadaStore() {
    String value = RandomStringUtils.randomAlphabetic(5);
    String key2 = "key2";
    String key3 = "key3";
    byte[] valueAsBytes = value.getBytes(StandardCharsets.UTF_8);
    String namespacedKey1 = CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, KEY1);
    String namespacedKey2 = CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson("namespace1", key2);
    String namespacedKey3 = CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson("namespace1", key3);
    Mockito.when(coordinatorStreamStore.all()).thenReturn(ImmutableMap.of(namespacedKey1, valueAsBytes, namespacedKey2, new byte[0], namespacedKey3, new byte[0]));

    Assert.assertEquals(ImmutableMap.of(KEY1, valueAsBytes), namespaceAwareCoordinatorStreamStore.all());
    Mockito.verify(coordinatorStreamStore).all();
  }
}
