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
package org.apache.samza.zk;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.testUtils.EmbeddedZookeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestZkMetadataStore {

  private static final String LOCALHOST = "127.0.0.1";

  private static final Random RANDOM = new Random();

  // In 101tec version, this constant was 1024 * 1024 * 10
  // The limit changed to 1024 * 1000 after helix migration.
  // A larger number would throw an exception.
  // See: https://github.com/apache/helix/blob/654636e54268907deb2e12d32913455cc543b436/zookeeper-api/src/main/java/org/apache/helix/zookeeper/zkclient/ZkClient.java#L2386
  // The limit can be set through a system property, but not in unit tests.
  // https://github.com/apache/helix/blob/654636e54268907deb2e12d32913455cc543b436/zookeeper-api/src/main/java/org/apache/helix/zookeeper/zkclient/ZkClient.java#L106
  private static final int VALUE_SIZE_IN_BYTES = 512000;

  private static EmbeddedZookeeper zkServer;

  private MetadataStore zkMetadataStore;

  @BeforeClass
  public static void beforeClass() {
    zkServer = new EmbeddedZookeeper();
    zkServer.setup();
  }

  @AfterClass
  public static void afterClass() {
    zkServer.teardown();
  }

  @Before
  public void beforeTest() {
    String testZkConnectionString = String.format("%s:%s", LOCALHOST, zkServer.getPort());
    Config zkConfig = new MapConfig(ImmutableMap.of(ZkConfig.ZK_CONNECT, testZkConnectionString));
    zkMetadataStore = new ZkMetadataStoreFactory().getMetadataStore(String.format("%s", RandomStringUtils.randomAlphabetic(5)), zkConfig, new MetricsRegistryMap());
  }

  @After
  public void afterTest() {
    zkMetadataStore.close();
  }

  @Test
  public void testReadAfterWrite() throws Exception {
    String key = "test-key1";
    byte[] value = getRandomByteArray(VALUE_SIZE_IN_BYTES);
    Assert.assertNull(zkMetadataStore.get(key));
    zkMetadataStore.put(key, value);
    Assert.assertTrue(Arrays.equals(value, zkMetadataStore.get(key)));
    Assert.assertEquals(1, zkMetadataStore.all().size());
  }

  @Test
  public void testReadAfterDelete() throws Exception {
    String key = "test-key1";
    byte[] value = getRandomByteArray(VALUE_SIZE_IN_BYTES);
    Assert.assertNull(zkMetadataStore.get(key));
    zkMetadataStore.put(key, value);
    Assert.assertTrue(Arrays.equals(value, zkMetadataStore.get(key)));
    zkMetadataStore.delete(key);
    Assert.assertNull(zkMetadataStore.get(key));
    Assert.assertEquals(0, zkMetadataStore.all().size());
  }

  @Test
  public void testReadOfNonExistentKey() {
    Assert.assertNull(zkMetadataStore.get("randomKey"));
    Assert.assertEquals(0, zkMetadataStore.all().size());
  }

  @Test
  public void testMultipleUpdatesForSameKey() throws Exception {
    String key = "test-key1";
    byte[] value = getRandomByteArray(VALUE_SIZE_IN_BYTES);
    byte[] value1 = getRandomByteArray(VALUE_SIZE_IN_BYTES);
    zkMetadataStore.put(key, value);
    zkMetadataStore.put(key, value1);
    Assert.assertTrue(Arrays.equals(value1, zkMetadataStore.get(key)));
    Assert.assertEquals(1, zkMetadataStore.all().size());
  }

  @Test
  public void testAllEntries() throws Exception {
    String key = "test-key1";
    String key1 = "test-key2";
    String key2 = "test-key3";
    byte[] value = getRandomByteArray(VALUE_SIZE_IN_BYTES);
    byte[] value1 = getRandomByteArray(VALUE_SIZE_IN_BYTES);
    byte[] value2 = getRandomByteArray(VALUE_SIZE_IN_BYTES);
    zkMetadataStore.put(key, value);
    zkMetadataStore.put(key1, value1);
    zkMetadataStore.put(key2, value2);
    ImmutableMap<String, byte[]> expected = ImmutableMap.of(key, value, key1, value1, key2, value2);
    Assert.assertEquals(expected.size(), zkMetadataStore.all().size());
  }

  private static byte[] getRandomByteArray(int numBytes) {
    byte[] byteArray = new byte[numBytes];
    RANDOM.nextBytes(byteArray);
    return byteArray;
  }
}
