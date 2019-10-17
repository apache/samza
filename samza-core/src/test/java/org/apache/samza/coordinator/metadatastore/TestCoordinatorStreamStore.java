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
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCoordinatorStreamStore {

  private static final String NAMESPACE = "namespace";
  private static final Config CONFIG = new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));

  private CoordinatorStreamStore coordinatorStreamStore;
  private NamespaceAwareCoordinatorStreamStore namespaceAwareCoordinatorStreamStore;

  @Before
  public void setup() {
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(CONFIG);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    namespaceAwareCoordinatorStreamStore = new NamespaceAwareCoordinatorStreamStore(coordinatorStreamStore, NAMESPACE);
  }

  @Test
  public void testReadAfterWrite() {
    String key = getCoordinatorMessageKey("test-key1");
    byte[] value = getValue("test-value1");
    Assert.assertNull(coordinatorStreamStore.get(key));
    coordinatorStreamStore.put(key, value);
    Assert.assertEquals(value, coordinatorStreamStore.get(key));
    Assert.assertEquals(1, namespaceAwareCoordinatorStreamStore.all().size());
  }

  @Test
  public void testReadAfterDelete() {
    String key = getCoordinatorMessageKey("test-key1");
    byte[] value = getValue("test-value1");
    Assert.assertNull(coordinatorStreamStore.get(key));
    coordinatorStreamStore.put(key, value);
    Assert.assertEquals(value, coordinatorStreamStore.get(key));
    coordinatorStreamStore.delete(key);
    Assert.assertNull(coordinatorStreamStore.get(key));
    Assert.assertEquals(0, namespaceAwareCoordinatorStreamStore.all().size());
  }

  @Test
  public void testReadOfNonExistentKey() {
    Assert.assertNull(coordinatorStreamStore.get("randomKey"));
    Assert.assertEquals(0, namespaceAwareCoordinatorStreamStore.all().size());
  }

  @Test
  public void testMultipleUpdatesForSameKey() {
    String key = getCoordinatorMessageKey("test-key1");
    byte[] value = getValue("test-value1");
    byte[] value1 = getValue("test-value2");
    coordinatorStreamStore.put(key, value);
    coordinatorStreamStore.put(key, value1);
    Assert.assertEquals(value1, coordinatorStreamStore.get(key));
    Assert.assertEquals(1, namespaceAwareCoordinatorStreamStore.all().size());
  }

  @Test
  public void testPutAll() {
    CoordinatorStreamStore spyCoordinatorStreamStore = Mockito.spy(coordinatorStreamStore);
    String key1 = getCoordinatorMessageKey("test-key1");
    String key2 = getCoordinatorMessageKey("test-key2");
    String key3 = getCoordinatorMessageKey("test-key3");
    String key4 = getCoordinatorMessageKey("test-key4");
    String key5 = getCoordinatorMessageKey("test-key5");
    byte[] value1 = getValue("test-value1");
    byte[] value2 = getValue("test-value2");
    byte[] value3 = getValue("test-value3");
    byte[] value4 = getValue("test-value4");
    byte[] value5 = getValue("test-value5");
    ImmutableMap<String, byte[]> map =
        ImmutableMap.of(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5);
    spyCoordinatorStreamStore.putAll(map);
    Assert.assertEquals(value1, spyCoordinatorStreamStore.get(key1));
    Assert.assertEquals(value2, spyCoordinatorStreamStore.get(key2));
    Assert.assertEquals(value3, spyCoordinatorStreamStore.get(key3));
    Assert.assertEquals(value4, spyCoordinatorStreamStore.get(key4));
    Assert.assertEquals(value5, spyCoordinatorStreamStore.get(key5));
    Mockito.verify(spyCoordinatorStreamStore).flush(); // verify flush called only once during putAll
  }

  @Test
  public void testAllEntries() {
    String key = getCoordinatorMessageKey("test-key1");
    String key1 = getCoordinatorMessageKey("test-key2");
    String key2 = getCoordinatorMessageKey("test-key3");
    byte[] value = getValue("test-value1");
    byte[] value1 = getValue("test-value2");
    byte[] value2 = getValue("test-value3");
    coordinatorStreamStore.put(key, value);
    coordinatorStreamStore.put(key1, value1);
    coordinatorStreamStore.put(key2, value2);
    ImmutableMap<String, byte[]> expected = ImmutableMap.of("test-key1", value, "test-key2", value1, "test-key3", value2);
    Assert.assertEquals(expected, namespaceAwareCoordinatorStreamStore.all());
  }

  private byte[] getValue(String value) {
    Serde<Map<String, Object>> messageSerde = new JsonSerde<>();
    SetTaskContainerMapping setTaskContainerMapping = new SetTaskContainerMapping("testSource", "testTask", value);
    return messageSerde.toBytes(setTaskContainerMapping.getMessageMap());
  }

  private static String getCoordinatorMessageKey(String key) {
    return CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(NAMESPACE, key);
  }
}
