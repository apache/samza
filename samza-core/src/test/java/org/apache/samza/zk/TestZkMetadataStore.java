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
    zkMetadataStore = new ZkMetadataStoreFactory().getMetadataStore(String.format("/%s", RandomStringUtils.randomAlphabetic(5)), zkConfig, new MetricsRegistryMap());
  }

  @After
  public void afterTest() {
    zkMetadataStore.close();
  }

  @Test
  public void testReadAfterWrite() throws Exception {
    byte[] key = "test-key1".getBytes("UTF-8");
    byte[] value = "test-value1".getBytes("UTF-8");
    Assert.assertNull(zkMetadataStore.get(key));
    zkMetadataStore.put(key, value);
    Assert.assertTrue(Arrays.equals(value, zkMetadataStore.get(key)));
    Assert.assertEquals(1, zkMetadataStore.all().size());
  }

  @Test
  public void testReadAfterDelete() throws Exception {
    byte[] key = "test-key1".getBytes("UTF-8");
    byte[] value = "test-value1".getBytes("UTF-8");
    Assert.assertNull(zkMetadataStore.get(key));
    zkMetadataStore.put(key, value);
    Assert.assertTrue(Arrays.equals(value, zkMetadataStore.get(key)));
    zkMetadataStore.delete(key);
    Assert.assertNull(zkMetadataStore.get(key));
    Assert.assertEquals(0, zkMetadataStore.all().size());
  }

  @Test
  public void testReadOfNonExistentKey() throws Exception {
    Assert.assertNull(zkMetadataStore.get("randomKey".getBytes("UTF-8")));
    Assert.assertEquals(0, zkMetadataStore.all().size());
  }

  @Test
  public void testMultipleUpdatesForSameKey() throws Exception {
    byte[] key = "test-key1".getBytes("UTF-8");
    byte[] value = "test-value1".getBytes("UTF-8");
    byte[] value1 = "test-value2".getBytes("UTF-8");
    zkMetadataStore.put(key, value);
    zkMetadataStore.put(key, value1);
    Assert.assertTrue(Arrays.equals(value1, zkMetadataStore.get(key)));
    Assert.assertEquals(1, zkMetadataStore.all().size());
  }

  @Test
  public void testAllEntries() throws Exception {
    byte[] key = "test-key1".getBytes("UTF-8");
    byte[] key1 = "test-key2".getBytes("UTF-8");
    byte[] key2 = "test-key3".getBytes("UTF-8");
    byte[] value = "test-value1".getBytes("UTF-8");
    byte[] value1 = "test-value2".getBytes("UTF-8");
    byte[] value2 = "test-value3".getBytes("UTF-8");
    zkMetadataStore.put(key, value);
    zkMetadataStore.put(key1, value1);
    zkMetadataStore.put(key2, value2);
    ImmutableMap<byte[], byte[]> expected = ImmutableMap.of(key, value, key1, value1, key2, value2);
    Assert.assertEquals(expected.size(), zkMetadataStore.all().size());
  }
}
