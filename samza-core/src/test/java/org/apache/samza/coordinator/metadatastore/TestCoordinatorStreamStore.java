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
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.messages.SetTaskContainerMapping;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CoordinatorStreamUtil.class)
public class TestCoordinatorStreamStore {

  private CoordinatorStreamStore coordinatorStreamStore;

  @Before
  public void setUp() {
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache();
    Map<String, String> configMap = ImmutableMap.of("job.name", "test-job",
                                                    "job.coordinator.system", "test-kafka");
    MockCoordinatorStreamSystemFactory systemFactory = new MockCoordinatorStreamSystemFactory();
    PowerMockito.mockStatic(CoordinatorStreamUtil.class);
    when(CoordinatorStreamUtil.getCoordinatorSystemFactory(anyObject())).thenReturn(systemFactory);
    when(CoordinatorStreamUtil.getCoordinatorSystemStream(anyObject())).thenReturn(new SystemStream("test-kafka", "test"));
    when(CoordinatorStreamUtil.getCoordinatorStreamName(anyObject(), anyObject())).thenReturn("test");
    coordinatorStreamStore = new CoordinatorStreamStore(SetTaskContainerMapping.TYPE, new MapConfig(configMap), new MetricsRegistryMap());
    coordinatorStreamStore.init();
  }

  @Test
  public void testReadAfterWrite() {
    String key = "test-key1";
    byte[] value = getValue("test-value1");
    Assert.assertNull(coordinatorStreamStore.get(key));
    coordinatorStreamStore.put(key, value);
    Assert.assertEquals(value, coordinatorStreamStore.get(key));
    Assert.assertEquals(1, coordinatorStreamStore.all().size());
  }

  @Test
  public void testReadAfterDelete() {
    String key = "test-key1";
    byte[] value = getValue("test-value1");
    Assert.assertNull(coordinatorStreamStore.get(key));
    coordinatorStreamStore.put(key, value);
    Assert.assertEquals(value, coordinatorStreamStore.get(key));
    coordinatorStreamStore.delete(key);
    Assert.assertNull(coordinatorStreamStore.get(key));
    Assert.assertEquals(0, coordinatorStreamStore.all().size());
  }

  @Test
  public void testReadOfNonExistentKey() {
    Assert.assertNull(coordinatorStreamStore.get("randomKey"));
    Assert.assertEquals(0, coordinatorStreamStore.all().size());
  }

  @Test
  public void testMultipleUpdatesForSameKey() {
    String key = "test-key1";
    byte[] value = getValue("test-value1");
    byte[] value1 = getValue("test-value2");
    coordinatorStreamStore.put(key, value);
    coordinatorStreamStore.put(key, value1);
    Assert.assertEquals(value1, coordinatorStreamStore.get(key));
    Assert.assertEquals(1, coordinatorStreamStore.all().size());
  }

  @Test
  public void testAllEntries() {
    String key = "test-key1";
    String key1 = "test-key2";
    String key2 = "test-key3";
    byte[] value = getValue("test-value1");
    byte[] value1 = getValue("test-value2");
    byte[] value2 = getValue("test-value3");
    coordinatorStreamStore.put(key, value);
    coordinatorStreamStore.put(key1, value1);
    coordinatorStreamStore.put(key2, value2);
    ImmutableMap<String, byte[]> expected = ImmutableMap.of(key, value, key1, value1, key2, value2);
    Assert.assertEquals(expected, coordinatorStreamStore.all());
  }

  private byte[] getValue(String value) {
    Serde<Map<String, Object>> messageSerde = new JsonSerde<>();
    SetTaskContainerMapping setTaskContainerMapping = new SetTaskContainerMapping("testSource", "testTask", value);
    return messageSerde.toBytes(setTaskContainerMapping.getMessageMap());
  }
}
