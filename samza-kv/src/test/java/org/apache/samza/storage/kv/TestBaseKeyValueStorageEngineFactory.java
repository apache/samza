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

import java.io.File;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.StorageEngine;
import org.apache.samza.storage.StorageEngineFactory;
import org.apache.samza.storage.StoreProperties;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TestBaseKeyValueStorageEngineFactory {
  private static final String STORE_NAME = "myStore";
  private static final StorageEngineFactory.StoreMode STORE_MODE = StorageEngineFactory.StoreMode.ReadWrite;
  private static final SystemStreamPartition CHANGELOG_SSP =
      new SystemStreamPartition("system", "stream", new Partition(0));
  private static final Map<String, String> BASE_CONFIG =
      ImmutableMap.of(String.format(StorageConfig.FACTORY, STORE_NAME),
          MockKeyValueStorageEngineFactory.class.getName());
  private static final Map<String, String> DISABLE_CACHE =
      ImmutableMap.of(String.format("stores.%s.object.cache.size", STORE_NAME), "0");
  private static final Map<String, String> DISALLOW_LARGE_MESSAGES =
      ImmutableMap.of(String.format(StorageConfig.DISALLOW_LARGE_MESSAGES, STORE_NAME), "true");
  private static final Map<String, String> DROP_LARGE_MESSAGES =
      ImmutableMap.of(String.format(StorageConfig.DROP_LARGE_MESSAGES, STORE_NAME), "true");
  private static final Map<String, String> ACCESS_LOG_ENABLED =
      ImmutableMap.of(String.format("stores.%s.accesslog.enabled", STORE_NAME), "true");

  @Mock
  private File storeDir;
  @Mock
  private Serde<String> keySerde;
  @Mock
  private Serde<String> msgSerde;
  @Mock
  private MessageCollector changelogCollector;
  @Mock
  private MetricsRegistry metricsRegistry;
  @Mock
  private JobContext jobContext;
  @Mock
  private ContainerContext containerContext;
  @Mock
  private KeyValueStore<byte[], byte[]> rawKeyValueStore;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    // some metrics objects need this for histogram metric instantiation
    when(this.metricsRegistry.newGauge(any(), any())).thenReturn(mock(Gauge.class));
  }

  @Test(expected = SamzaException.class)
  public void testMissingStoreFactory() {
    Config config = new MapConfig();
    callGetStorageEngine(config, null);
  }

  @Test(expected = SamzaException.class)
  public void testInvalidCacheSize() {
    Config config = new MapConfig(BASE_CONFIG,
        ImmutableMap.of(String.format("stores.%s.write.cache.batch", STORE_NAME), "100",
            String.format("stores.%s.object.cache.size", STORE_NAME), "50"));
    callGetStorageEngine(config, null);
  }

  @Test(expected = SamzaException.class)
  public void testMissingKeySerde() {
    Config config = new MapConfig(BASE_CONFIG);
    when(this.jobContext.getConfig()).thenReturn(config);
    new MockKeyValueStorageEngineFactory(this.rawKeyValueStore).getStorageEngine(STORE_NAME, this.storeDir, null,
        this.msgSerde, this.changelogCollector, this.metricsRegistry, null, this.jobContext, this.containerContext,
        STORE_MODE);
  }

  @Test(expected = SamzaException.class)
  public void testMissingValueSerde() {
    Config config = new MapConfig(BASE_CONFIG);
    when(this.jobContext.getConfig()).thenReturn(config);
    new MockKeyValueStorageEngineFactory(this.rawKeyValueStore).getStorageEngine(STORE_NAME, this.storeDir,
        this.keySerde, null, this.changelogCollector, this.metricsRegistry, null, this.jobContext,
        this.containerContext, STORE_MODE);
  }

  @Test
  public void testInMemoryKeyValueStore() {
    Config config = new MapConfig(DISABLE_CACHE, ImmutableMap.of(String.format(StorageConfig.FACTORY, STORE_NAME),
        "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory"));
    StorageEngine storageEngine = callGetStorageEngine(config, null);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), false, false, false);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(nullSafeKeyValueStore.getStore(), SerializedKeyValueStore.class);
    // config has the in-memory key-value factory, but still calling the test factory, so store will be the test store
    assertEquals(this.rawKeyValueStore, serializedKeyValueStore.getStore());
  }

  @Test
  public void testDurableKeyValueStore() {
    Config config = new MapConfig(BASE_CONFIG, DISABLE_CACHE,
        ImmutableMap.of(String.format(StorageConfig.STORE_BACKEND_BACKUP_FACTORIES, STORE_NAME),
        "backendFactory,backendFactory2"));
    StorageEngine storageEngine = callGetStorageEngine(config, null);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), true, false, true);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(nullSafeKeyValueStore.getStore(), SerializedKeyValueStore.class);
    // config has the in-memory key-value factory, but still calling the test factory, so store will be the test store
    assertEquals(this.rawKeyValueStore, serializedKeyValueStore.getStore());
  }

  @Test
  public void testRawStoreOnly() {
    Config config = new MapConfig(BASE_CONFIG, DISABLE_CACHE);
    StorageEngine storageEngine = callGetStorageEngine(config, null);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), true, false, false);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(nullSafeKeyValueStore.getStore(), SerializedKeyValueStore.class);
    assertEquals(this.rawKeyValueStore, serializedKeyValueStore.getStore());
  }

  @Test
  public void testWithLoggedStore() {
    Config config = new MapConfig(BASE_CONFIG, DISABLE_CACHE);
    StorageEngine storageEngine = callGetStorageEngine(config, CHANGELOG_SSP);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), true, true, false);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(nullSafeKeyValueStore.getStore(), SerializedKeyValueStore.class);
    LoggedStore<?, ?> loggedStore = assertAndCast(serializedKeyValueStore.getStore(), LoggedStore.class);
    // type generics don't match due to wildcard type, but checking reference equality, so type generics don't matter
    // noinspection AssertEqualsBetweenInconvertibleTypes
    assertEquals(this.rawKeyValueStore, loggedStore.getStore());
  }

  @Test
  public void testWithLoggedStoreAndCachedStore() {
    Config config = new MapConfig(BASE_CONFIG);
    StorageEngine storageEngine = callGetStorageEngine(config, CHANGELOG_SSP);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), true, true, false);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    CachedStore<?, ?> cachedStore = assertAndCast(nullSafeKeyValueStore.getStore(), CachedStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(cachedStore.getStore(), SerializedKeyValueStore.class);
    LoggedStore<?, ?> loggedStore = assertAndCast(serializedKeyValueStore.getStore(), LoggedStore.class);
    // type generics don't match due to wildcard type, but checking reference equality, so type generics don't matter
    // noinspection AssertEqualsBetweenInconvertibleTypes
    assertEquals(this.rawKeyValueStore, loggedStore.getStore());
  }

  @Test
  public void testDisallowLargeMessages() {
    Config config = new MapConfig(BASE_CONFIG, DISABLE_CACHE, DISALLOW_LARGE_MESSAGES);
    StorageEngine storageEngine = callGetStorageEngine(config, null);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), true, false, false);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(nullSafeKeyValueStore.getStore(), SerializedKeyValueStore.class);
    LargeMessageSafeStore largeMessageSafeStore =
        assertAndCast(serializedKeyValueStore.getStore(), LargeMessageSafeStore.class);
    assertEquals(this.rawKeyValueStore, largeMessageSafeStore.getStore());
  }

  @Test
  public void testDisallowLargeMessagesWithCache() {
    Config config = new MapConfig(BASE_CONFIG, DISALLOW_LARGE_MESSAGES);
    StorageEngine storageEngine = callGetStorageEngine(config, null);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), true, false, false);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(nullSafeKeyValueStore.getStore(), SerializedKeyValueStore.class);
    LargeMessageSafeStore largeMessageSafeStore =
        assertAndCast(serializedKeyValueStore.getStore(), LargeMessageSafeStore.class);
    CachedStore<?, ?> cachedStore = assertAndCast(largeMessageSafeStore.getStore(), CachedStore.class);
    // type generics don't match due to wildcard type, but checking reference equality, so type generics don't matter
    // noinspection AssertEqualsBetweenInconvertibleTypes
    assertEquals(this.rawKeyValueStore, cachedStore.getStore());
  }

  @Test
  public void testDropLargeMessages() {
    Config config = new MapConfig(BASE_CONFIG, DISABLE_CACHE, DROP_LARGE_MESSAGES);
    StorageEngine storageEngine = callGetStorageEngine(config, null);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), true, false, false);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(nullSafeKeyValueStore.getStore(), SerializedKeyValueStore.class);
    LargeMessageSafeStore largeMessageSafeStore =
        assertAndCast(serializedKeyValueStore.getStore(), LargeMessageSafeStore.class);
    assertEquals(this.rawKeyValueStore, largeMessageSafeStore.getStore());
  }

  @Test
  public void testDropLargeMessagesWithCache() {
    Config config = new MapConfig(BASE_CONFIG, DROP_LARGE_MESSAGES);
    StorageEngine storageEngine = callGetStorageEngine(config, null);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), true, false, false);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    CachedStore<?, ?> cachedStore = assertAndCast(nullSafeKeyValueStore.getStore(), CachedStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(cachedStore.getStore(), SerializedKeyValueStore.class);
    LargeMessageSafeStore largeMessageSafeStore =
        assertAndCast(serializedKeyValueStore.getStore(), LargeMessageSafeStore.class);
    assertEquals(this.rawKeyValueStore, largeMessageSafeStore.getStore());
  }

  @Test
  public void testAccessLogStore() {
    Config config = new MapConfig(BASE_CONFIG, DISABLE_CACHE, ACCESS_LOG_ENABLED);
    // AccessLoggedStore requires a changelog SSP
    StorageEngine storageEngine = callGetStorageEngine(config, CHANGELOG_SSP);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = baseStorageEngineValidation(storageEngine);
    assertStoreProperties(keyValueStorageEngine.getStoreProperties(), true, true, false);
    NullSafeKeyValueStore<?, ?> nullSafeKeyValueStore =
        assertAndCast(keyValueStorageEngine.getWrapperStore(), NullSafeKeyValueStore.class);
    AccessLoggedStore<?, ?> accessLoggedStore =
        assertAndCast(nullSafeKeyValueStore.getStore(), AccessLoggedStore.class);
    SerializedKeyValueStore<?, ?> serializedKeyValueStore =
        assertAndCast(accessLoggedStore.getStore(), SerializedKeyValueStore.class);
    LoggedStore<?, ?> loggedStore = assertAndCast(serializedKeyValueStore.getStore(), LoggedStore.class);
    // type generics don't match due to wildcard type, but checking reference equality, so type generics don't matter
    // noinspection AssertEqualsBetweenInconvertibleTypes
    assertEquals(this.rawKeyValueStore, loggedStore.getStore());
  }

  private static <T extends KeyValueStore<?, ?>> T assertAndCast(KeyValueStore<?, ?> keyValueStore, Class<T> clazz) {
    assertTrue("Expected type " + clazz.getName(), clazz.isInstance(keyValueStore));
    return clazz.cast(keyValueStore);
  }

  private KeyValueStorageEngine<?, ?> baseStorageEngineValidation(StorageEngine storageEngine) {
    assertTrue(storageEngine instanceof KeyValueStorageEngine);
    KeyValueStorageEngine<?, ?> keyValueStorageEngine = (KeyValueStorageEngine<?, ?>) storageEngine;
    assertEquals(this.rawKeyValueStore, keyValueStorageEngine.getRawStore());
    return keyValueStorageEngine;
  }

  private static void assertStoreProperties(StoreProperties storeProperties, boolean expectedPersistedToDisk,
      boolean expectedLoggedStore, boolean expectedDurable) {
    assertEquals(expectedPersistedToDisk, storeProperties.isPersistedToDisk());
    assertEquals(expectedLoggedStore, storeProperties.isLoggedStore());
    assertEquals(expectedDurable, storeProperties.isDurableStore());
  }

  /**
   * @param changelogSSP if non-null, then enables logged store
   */
  private StorageEngine callGetStorageEngine(Config config, SystemStreamPartition changelogSSP) {
    when(this.jobContext.getConfig()).thenReturn(config);
    return new MockKeyValueStorageEngineFactory(this.rawKeyValueStore).getStorageEngine(STORE_NAME, this.storeDir,
        this.keySerde, this.msgSerde, this.changelogCollector, this.metricsRegistry, changelogSSP, this.jobContext,
        this.containerContext, STORE_MODE);
  }
}
