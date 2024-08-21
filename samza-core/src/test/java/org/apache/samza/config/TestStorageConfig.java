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

package org.apache.samza.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemStream;
import org.junit.Test;

import static org.apache.samza.config.StorageConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestStorageConfig {
  private static final String STORE_NAME0 = "store0";
  private static final String STORE_NAME1 = "store1";
  private static final String STORE_NAME2 = "store2";
  private static final String STORE_NAME3 = "store3";

  @Test
  public void testGetStoreNames() {
    // empty config, so no stores
    assertEquals(Collections.emptyList(), new StorageConfig(new MapConfig()).getStoreNames());

    Set<String> expectedStoreNames = ImmutableSet.of(STORE_NAME0, STORE_NAME1);
    // has stores
    StorageConfig storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.FACTORY, STORE_NAME0), "store0.factory.class",
            String.format(StorageConfig.FACTORY, STORE_NAME1), "store1.factory.class")));
    List<String> actual = storageConfig.getStoreNames();
    // ordering shouldn't matter
    assertEquals(2, actual.size());
    assertEquals(expectedStoreNames, ImmutableSet.copyOf(actual));

    //has side input stores
    StorageConfig config = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(FACTORY, STORE_NAME0), "store0.factory.class",
            String.format(StorageConfig.SIDE_INPUTS_PROCESSOR_FACTORY, STORE_NAME1), "store1.factory.class")));

    actual = config.getStoreNames();

    assertEquals(2, actual.size());
    assertEquals(expectedStoreNames, ImmutableSet.copyOf(actual));
  }

  @Test
  public void testGetStoreNamesDoesNotReturnDuplicatesForSideInputs() {
    // empty config, so no stores
    assertEquals(Collections.emptyList(), new StorageConfig(new MapConfig()).getStoreNames());

    Set<String> expectedStoreNames = ImmutableSet.of(STORE_NAME0, STORE_NAME1);
    // has stores
    StorageConfig storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.FACTORY, STORE_NAME0), "store0.factory.class",
            String.format(StorageConfig.FACTORY, STORE_NAME1), "store1.factory.class",
            String.format(StorageConfig.SIDE_INPUTS_PROCESSOR_FACTORY, STORE_NAME1), "store1.side.inputs.processor")));

    List<String> actual = storageConfig.getStoreNames();
    // ordering shouldn't matter
    assertEquals(2, actual.size());
    assertEquals(expectedStoreNames, ImmutableSet.copyOf(actual));

    //has side input stores
    StorageConfig config = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(FACTORY, STORE_NAME0), "store0.factory.class",
            String.format(StorageConfig.SIDE_INPUTS_PROCESSOR_FACTORY, STORE_NAME1), "store1.factory.class")));

    actual = storageConfig.getStoreNames();

    assertEquals(2, actual.size());
    assertEquals(expectedStoreNames, ImmutableSet.copyOf(actual));
  }

  /**
   * Test verifies that the {@link StorageConfig#STORE_RESTORE_FACTORIES} which matches pattern for store.%s.factory
   * is not picked up as in store names list
   */
  @Test
  public void testGetStoreNamesIgnoreStateRestoreFactory() {
    // empty config, so no stores
    assertEquals(Collections.emptyList(), new StorageConfig(new MapConfig()).getStoreNames());

    Set<String> expectedStoreNames = ImmutableSet.of(STORE_NAME0, STORE_NAME1);
    // has stores
    StorageConfig storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.FACTORY, STORE_NAME0), "store0.factory.class",
            String.format(StorageConfig.FACTORY, STORE_NAME1), "store1.factory.class", STORE_RESTORE_FACTORIES, "org.apache.class")));

    List<String> actual = storageConfig.getStoreNames();
    // ordering shouldn't matter
    assertEquals(2, actual.size());
    assertEquals(expectedStoreNames, ImmutableSet.copyOf(actual));
  }

  @Test
  public void testGetChangelogStream() {
    // empty config, so no changelog stream
    assertEquals(Optional.empty(), new StorageConfig(new MapConfig()).getChangelogStream(STORE_NAME0));

    // store has empty string for changelog stream
    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(StorageConfig.CHANGELOG_STREAM, STORE_NAME0), "")));
    assertEquals(Optional.empty(), storageConfig.getChangelogStream(STORE_NAME0));
    assertEquals(Collections.emptyMap(), storageConfig.getStoreChangelogs());

    // store has full changelog system-stream defined
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.CHANGELOG_STREAM, STORE_NAME0),
            "changelog-system.changelog-stream0", String.format(FACTORY, STORE_NAME0), "store0.factory.class")));
    assertEquals(Optional.of("changelog-system.changelog-stream0"), storageConfig.getChangelogStream(STORE_NAME0));
    assertEquals(ImmutableMap.of(STORE_NAME0, new SystemStream("changelog-system", "changelog-stream0")), storageConfig.getStoreChangelogs());

    // store has changelog stream defined, but system comes from job.changelog.system
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.CHANGELOG_STREAM, STORE_NAME0), "changelog-stream0",
            String.format(FACTORY, STORE_NAME0), "store0.factory.class",
            StorageConfig.CHANGELOG_SYSTEM, "changelog-system")));
    assertEquals(Optional.of("changelog-system.changelog-stream0"), storageConfig.getChangelogStream(STORE_NAME0));
    assertEquals(ImmutableMap.of(STORE_NAME0,  new SystemStream("changelog-system", "changelog-stream0")), storageConfig.getStoreChangelogs());

    // batch mode: create unique stream name
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.CHANGELOG_STREAM, STORE_NAME0),
            "changelog-system.changelog-stream0", String.format(FACTORY, STORE_NAME0), "store0.factory.class",
            ApplicationConfig.APP_MODE,
            ApplicationConfig.ApplicationMode.BATCH.name().toLowerCase(), ApplicationConfig.APP_RUN_ID, "run-id")));
    assertEquals(Optional.of("changelog-system.changelog-stream0-run-id"),
        storageConfig.getChangelogStream(STORE_NAME0));
    assertEquals(ImmutableMap.of(STORE_NAME0,  new SystemStream("changelog-system", "changelog-stream0-run-id")), storageConfig.getStoreChangelogs());

    // job has no changelog stream defined
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(StorageConfig.CHANGELOG_SYSTEM, "changelog-system", JobConfig.JOB_DEFAULT_SYSTEM,
            "should-not-be-used", String.format(FACTORY, STORE_NAME0), "store0.factory.class")));
    assertEquals(Optional.empty(), storageConfig.getChangelogStream(STORE_NAME0));
    assertEquals(Collections.emptyMap(), storageConfig.getStoreChangelogs());

    // job.changelog.system takes precedence over job.default.system when changelog is specified as just streamName
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(StorageConfig.CHANGELOG_SYSTEM, "changelog-system", JobConfig.JOB_DEFAULT_SYSTEM,
            "should-not-be-used", String.format(CHANGELOG_STREAM, STORE_NAME0), "streamName",
            String.format(FACTORY, STORE_NAME0), "store0.factory.class")));
    assertEquals("changelog-system.streamName", storageConfig.getChangelogStream(STORE_NAME0).get());
    assertEquals(ImmutableMap.of(STORE_NAME0,  new SystemStream("changelog-system", "streamName")), storageConfig.getStoreChangelogs());

    // job.changelog.system takes precedence over job.default.system when changelog is specified as {systemName}.{streamName}
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(StorageConfig.CHANGELOG_SYSTEM, "changelog-system", JobConfig.JOB_DEFAULT_SYSTEM,
            "should-not-be-used", String.format(CHANGELOG_STREAM, STORE_NAME0), "changelog-system.streamName",
            String.format(FACTORY, STORE_NAME0), "store0.factory.class")));
    assertEquals("changelog-system.streamName", storageConfig.getChangelogStream(STORE_NAME0).get());
    assertEquals(ImmutableMap.of(STORE_NAME0,  new SystemStream("changelog-system", "streamName")), storageConfig.getStoreChangelogs());

    // systemName specified using stores.{storeName}.changelog = {systemName}.{streamName} should take precedence even
    // when job.changelog.system and job.default.system are specified
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(StorageConfig.CHANGELOG_SYSTEM, "default-changelog-system",
            JobConfig.JOB_DEFAULT_SYSTEM, "default-system",
            String.format(CHANGELOG_STREAM, STORE_NAME0), "nondefault-changelog-system.streamName",
            String.format(FACTORY, STORE_NAME0), "store0.factory.class")));
    assertEquals("nondefault-changelog-system.streamName", storageConfig.getChangelogStream(STORE_NAME0).get());
    assertEquals(ImmutableMap.of(STORE_NAME0,  new SystemStream("nondefault-changelog-system", "streamName")), storageConfig.getStoreChangelogs());

    // fall back to job.default.system if job.changelog.system is not specified
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(JobConfig.JOB_DEFAULT_SYSTEM, "default-system", String.format(CHANGELOG_STREAM, STORE_NAME0),
            "streamName", String.format(FACTORY, STORE_NAME0), "store0.factory.class")));
    assertEquals("default-system.streamName", storageConfig.getChangelogStream(STORE_NAME0).get());
    assertEquals(ImmutableMap.of(STORE_NAME0,  new SystemStream("default-system", "streamName")), storageConfig.getStoreChangelogs());
  }

  @Test(expected = SamzaException.class)
  public void testGetChangelogStreamMissingSystem() {
    StorageConfig storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.CHANGELOG_STREAM, STORE_NAME0), "changelog-stream0")));
    storageConfig.getChangelogStream(STORE_NAME0);
  }

  @Test
  public void testGetBackupManagerFactories() {
    String factory1 = "factory1";
    String factory2 = "factory2";
    String factory3 = "factory3";
    StorageConfig storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(
            String.format(STORE_BACKUP_FACTORIES, STORE_NAME0), factory1 + "," + factory2,
            String.format(STORE_BACKUP_FACTORIES, STORE_NAME1), factory1,
            String.format(STORE_BACKUP_FACTORIES, STORE_NAME2), factory3,
            // store_name3 should use DEFAULT_STATE_BACKEND_FACTORY due to changelog presence
            String.format(CHANGELOG_STREAM, STORE_NAME3), "nondefault-changelog-system.streamName"),
        ImmutableMap.of(
            String.format(FACTORY, STORE_NAME0), "store0.factory.class",
            String.format(FACTORY, STORE_NAME1), "store1.factory.class",
            String.format(FACTORY, STORE_NAME2), "store2.factory.class",
            String.format(FACTORY, STORE_NAME3), "store3.factory.class",
            // this store should have no backend factory configured
            String.format(FACTORY, "noFactoryStore"), "noFactory.factory.class"
        )
    ));
    Set<String> factories = storageConfig.getBackupFactories();
    assertTrue(factories.contains(factory1));
    assertTrue(factories.contains(factory2));
    assertTrue(factories.contains(factory3));
    assertTrue(factories.contains(KAFKA_STATE_BACKEND_FACTORY));
    assertEquals(4, factories.size());
    assertEquals(ImmutableList.of(factory1, factory2), storageConfig.getStoreBackupFactories(STORE_NAME0));
    assertEquals(ImmutableList.of(factory1), storageConfig.getStoreBackupFactories(STORE_NAME1));
    assertEquals(ImmutableList.of(factory3), storageConfig.getStoreBackupFactories(STORE_NAME2));
    assertEquals(DEFAULT_BACKUP_FACTORIES, storageConfig.getStoreBackupFactories(STORE_NAME3));
    assertTrue(storageConfig.getStoreBackupFactories("emptyStore").isEmpty());
    assertTrue(storageConfig.getStoreBackupFactories("noFactoryStore").isEmpty());
  }

  @Test
  public void testGetStoreToBackup() {
    String targetFactory = "target.class";
    StorageConfig config = new StorageConfig(new MapConfig(
        ImmutableMap.of(
            String.format(StorageConfig.STORE_BACKUP_FACTORIES, STORE_NAME0), targetFactory,
            String.format(StorageConfig.STORE_BACKUP_FACTORIES, STORE_NAME1), targetFactory + "," +
                KAFKA_STATE_BACKEND_FACTORY,
            String.format(StorageConfig.STORE_BACKUP_FACTORIES, STORE_NAME2), KAFKA_STATE_BACKEND_FACTORY),
        ImmutableMap.of(
            String.format(FACTORY, STORE_NAME0), "store0.factory.class",
            String.format(FACTORY, STORE_NAME1), "store1.factory.class",
            String.format(FACTORY, STORE_NAME2), "store2.factory.class",
            String.format(FACTORY, STORE_NAME3), "store3.factory.class",
            String.format(CHANGELOG_STREAM, STORE_NAME3), "nondefault-changelog-system.streamName"
        )
    ));

    List<String> targetStoreNames = config.getStoresWithBackupFactory(targetFactory);
    List<String> defaultStoreNames = config.getStoresWithBackupFactory(
        KAFKA_STATE_BACKEND_FACTORY);
    assertTrue(targetStoreNames.containsAll(ImmutableList.of(STORE_NAME0, STORE_NAME1)));
    assertEquals(2, targetStoreNames.size());
    assertTrue(defaultStoreNames.containsAll(ImmutableList.of(STORE_NAME2, STORE_NAME1, STORE_NAME3)));
    assertEquals(3, defaultStoreNames.size());
  }

  @Test
  public void testGetAccessLogEnabled() {
    // empty config, access log disabled
    assertFalse(new StorageConfig(new MapConfig()).getAccessLogEnabled(STORE_NAME0));

    assertFalse(new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.ACCESSLOG_ENABLED, STORE_NAME0), "false"))).getAccessLogEnabled(
        STORE_NAME0));

    assertTrue(new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.ACCESSLOG_ENABLED, STORE_NAME0), "true"))).getAccessLogEnabled(
        STORE_NAME0));
  }

  @Test
  public void testGetAccessLogStream() {
    String changelogStream = "changelog-stream";
    assertEquals(changelogStream + "-" + StorageConfig.ACCESSLOG_STREAM_SUFFIX,
        new StorageConfig(new MapConfig()).getAccessLogStream(changelogStream));
  }

  @Test
  public void testGetAccessLogSamplingRatio() {
    // empty config, return default sampling ratio
    assertEquals(StorageConfig.DEFAULT_ACCESSLOG_SAMPLING_RATIO,
        new StorageConfig(new MapConfig()).getAccessLogSamplingRatio(STORE_NAME0));

    assertEquals(40, new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.ACCESSLOG_SAMPLING_RATIO, STORE_NAME0),
            "40"))).getAccessLogSamplingRatio(STORE_NAME0));
  }

  @Test
  public void testGetStorageFactoryClassName() {
    // empty config, so no factory
    assertEquals(Optional.empty(), new StorageConfig(new MapConfig()).getStorageFactoryClassName(STORE_NAME0));

    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(StorageConfig.FACTORY, STORE_NAME0), "my.factory.class")));
    assertEquals(Optional.of("my.factory.class"), storageConfig.getStorageFactoryClassName(STORE_NAME0));
  }

  @Test
  public void testGetStorageKeySerde() {
    // empty config, so no key serde
    assertEquals(Optional.empty(), new StorageConfig(new MapConfig()).getStorageKeySerde(STORE_NAME0));

    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(StorageConfig.KEY_SERDE, STORE_NAME0), "my.key.serde.class")));
    assertEquals(Optional.of("my.key.serde.class"), storageConfig.getStorageKeySerde(STORE_NAME0));
  }

  @Test
  public void testGetStorageMsgSerde() {
    // empty config, so no msg serde
    assertEquals(Optional.empty(), new StorageConfig(new MapConfig()).getStorageMsgSerde(STORE_NAME0));

    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(StorageConfig.MSG_SERDE, STORE_NAME0), "my.msg.serde.class")));
    assertEquals(Optional.of("my.msg.serde.class"), storageConfig.getStorageMsgSerde(STORE_NAME0));
  }

  @Test
  public void testGetSideInputs() {
    // empty config, so no system
    assertEquals(Collections.emptyList(), new StorageConfig(new MapConfig()).getSideInputs(STORE_NAME0));

    // single side input
    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(StorageConfig.SIDE_INPUTS, STORE_NAME0), "side-input")));
    assertEquals(Collections.singletonList("side-input"), storageConfig.getSideInputs(STORE_NAME0));

    // multiple side inputs
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.SIDE_INPUTS, STORE_NAME0), "side-input0,side-input1")));
    assertEquals(ImmutableList.of("side-input0", "side-input1"), storageConfig.getSideInputs(STORE_NAME0));

    // ignore whitespace
    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.SIDE_INPUTS, STORE_NAME0), ", side-input0 ,,side-input1,")));
    assertEquals(ImmutableList.of("side-input0", "side-input1"), storageConfig.getSideInputs(STORE_NAME0));
  }

  @Test
  public void testGetSideInputsProcessorFactory() {
    // empty config, so no factory
    assertEquals(Optional.empty(), new StorageConfig(new MapConfig()).getSideInputsProcessorFactory(STORE_NAME0));

    StorageConfig storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.SIDE_INPUTS_PROCESSOR_FACTORY, STORE_NAME0),
            "my.side.inputs.factory.class")));
    assertEquals(Optional.of("my.side.inputs.factory.class"), storageConfig.getSideInputsProcessorFactory(STORE_NAME0));
  }

  @Test
  public void testGetSideInputsProcessorSerializedInstance() {
    // empty config, so no factory
    assertEquals(Optional.empty(),
        new StorageConfig(new MapConfig()).getSideInputsProcessorSerializedInstance(STORE_NAME0));

    StorageConfig storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.SIDE_INPUTS_PROCESSOR_SERIALIZED_INSTANCE, STORE_NAME0),
            "serialized_instance")));
    assertEquals(Optional.of("serialized_instance"),
        storageConfig.getSideInputsProcessorSerializedInstance(STORE_NAME0));
  }

  @Test
  public void testGetChangeLogDeleteRetentionInMs() {
    // empty config, return default sampling ratio
    assertEquals(StorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS,
        new StorageConfig(new MapConfig()).getChangeLogDeleteRetentionInMs(STORE_NAME0));

    StorageConfig storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.CHANGELOG_DELETE_RETENTION_MS, STORE_NAME0),
            Long.toString(StorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS * 2))));
    assertEquals(StorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS * 2,
        storageConfig.getChangeLogDeleteRetentionInMs(STORE_NAME0));
  }

  @Test
  public void testHasDurableStores() {
    // no changelog, which means no durable stores
    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(StorageConfig.FACTORY, STORE_NAME0), "factory.class")));
    assertFalse(storageConfig.hasDurableStores());

    storageConfig = new StorageConfig(new MapConfig(
        ImmutableMap.of(String.format(StorageConfig.FACTORY, STORE_NAME0), "factory.class",
            String.format(CHANGELOG_STREAM, STORE_NAME0), "system0.changelog-stream")));
    assertTrue(storageConfig.hasDurableStores());
  }

  @Test
  public void testGetChangelogMaxMsgSizeBytes() {
    // empty config, return default size
    assertEquals(StorageConfig.DEFAULT_CHANGELOG_MAX_MSG_SIZE_BYTES, new StorageConfig(new MapConfig()).getChangelogMaxMsgSizeBytes(STORE_NAME0));

    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(StorageConfig.CHANGELOG_MAX_MSG_SIZE_BYTES, STORE_NAME0), "10")));
    assertEquals(10, storageConfig.getChangelogMaxMsgSizeBytes(STORE_NAME0));
  }

  @Test
  public void testGetDisallowLargeMessages() {
    // empty config, return default size
    assertEquals(StorageConfig.DEFAULT_DISALLOW_LARGE_MESSAGES, new StorageConfig(new MapConfig()).getDisallowLargeMessages(STORE_NAME0));

    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(StorageConfig.DISALLOW_LARGE_MESSAGES, STORE_NAME0), "true")));
    assertEquals(true, storageConfig.getDisallowLargeMessages(STORE_NAME0));
  }

  @Test
  public void testGetDropLargeMessages() {
    // empty config, return default size
    assertEquals(StorageConfig.DEFAULT_DROP_LARGE_MESSAGES, new StorageConfig(new MapConfig()).getDropLargeMessages(STORE_NAME0));

    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(StorageConfig.DROP_LARGE_MESSAGES, STORE_NAME0), "true")));
    assertEquals(true, storageConfig.getDropLargeMessages(STORE_NAME0));
  }

  @Test
  public void testGetChangelogMinCompactionLagMs() {
    // empty config, return default lag ms
    Map<String, String> configMap = new HashMap<>();
    assertEquals(DEFAULT_CHANGELOG_MIN_COMPACTION_LAG_MS,
        new StorageConfig(new MapConfig(configMap)).getChangelogMinCompactionLagMs(STORE_NAME0));

    // override with configured default
    long defaultLagOverride = TimeUnit.HOURS.toMillis(8);
    configMap.put(String.format(CHANGELOG_MIN_COMPACTION_LAG_MS, "default"), String.valueOf(defaultLagOverride));
    assertEquals(defaultLagOverride, new StorageConfig(new MapConfig(configMap)).getChangelogMinCompactionLagMs(STORE_NAME0));

    // override for specific store
    long storeSpecificLagOverride = TimeUnit.HOURS.toMillis(6);
    configMap.put(String.format(CHANGELOG_MIN_COMPACTION_LAG_MS, STORE_NAME0), String.valueOf(storeSpecificLagOverride));
    assertEquals(storeSpecificLagOverride, new StorageConfig(new MapConfig(configMap)).getChangelogMinCompactionLagMs(STORE_NAME0));
  }

  @Test
  public void testGetRestoreManagers() {
    String storeName = "store1";
    String storeName2 = "store2";
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(FACTORY, storeName), "store1.factory.class");
    configMap.put(String.format(FACTORY, storeName2), "store2.factory.class");

    // empty config, return no restore managers
    assertEquals(Collections.emptySet(), new StorageConfig(new MapConfig(configMap)).getRestoreFactories());
    assertEquals(Collections.emptyList(), new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName));
    assertEquals(Collections.emptyList(), new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName2));

    // changelog set, should default to kafka state backend restore
    String changelogStreamOverride = "changelogStream";
    configMap.put(String.format(CHANGELOG_STREAM, storeName), changelogStreamOverride);
    configMap.put(String.format(CHANGELOG_STREAM, storeName2), changelogStreamOverride);
    configMap.put(StorageConfig.CHANGELOG_SYSTEM, "changelog-system");
    configMap.put(String.format(StorageConfig.CHANGELOG_STREAM, storeName), "changelog-stream0");
    assertEquals(ImmutableSet.of(KAFKA_STATE_BACKEND_FACTORY), new StorageConfig(new MapConfig(configMap)).getRestoreFactories());
    assertEquals(DEFAULT_RESTORE_FACTORIES, new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName));
    assertEquals(DEFAULT_RESTORE_FACTORIES, new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName2));

    // job restore manager config set should override to job backend factory
    String jobRestoreFactory1 = "jobBackendRestoreFactory1";
    String jobRestoreFactory2 = "jobBackendRestoreFactory2";
    String jobRestoreFactoryOverride = jobRestoreFactory1 + "," + jobRestoreFactory2;
    configMap.put(JOB_RESTORE_FACTORIES, jobRestoreFactoryOverride);
    assertEquals(ImmutableSet.of(jobRestoreFactory1, jobRestoreFactory2),
        new StorageConfig(new MapConfig(configMap)).getRestoreFactories());
    assertEquals(ImmutableList.of(jobRestoreFactory1, jobRestoreFactory2),
        new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName));
    assertEquals(ImmutableList.of(jobRestoreFactory1, jobRestoreFactory2),
        new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName2));

    // store specific restore managers set
    String storeRestoreFactory1 = "storeBackendRestoreFactory1";
    String storeRestoreFactory2 = "storeBackendRestoreFactory2";
    String storeRestoreFactoryOverride = storeRestoreFactory1 + "," + storeRestoreFactory2;
    configMap.put(String.format(STORE_RESTORE_FACTORIES, storeName), storeRestoreFactoryOverride);
    assertEquals(ImmutableSet.of(jobRestoreFactory1, jobRestoreFactory2, storeRestoreFactory1, storeRestoreFactory2),
        new StorageConfig(new MapConfig(configMap)).getRestoreFactories());
    assertEquals(ImmutableList.of(storeRestoreFactory1, storeRestoreFactory2),
        new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName));
    assertEquals(ImmutableList.of(jobRestoreFactory1, jobRestoreFactory2),
        new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName2));

    String emptyBackupFactory = "";
    configMap.put(String.format(STORE_RESTORE_FACTORIES, storeName), emptyBackupFactory);
    assertEquals(ImmutableSet.of(jobRestoreFactory1, jobRestoreFactory2),
        new StorageConfig(new MapConfig(configMap)).getRestoreFactories());
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName));
    assertEquals(ImmutableList.of(jobRestoreFactory1, jobRestoreFactory2),
        new StorageConfig(new MapConfig(configMap)).getStoreRestoreFactories(storeName2));
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoresWithRestoreFactory(KAFKA_STATE_BACKEND_FACTORY));
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoresWithRestoreFactory(storeRestoreFactory1));
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoresWithRestoreFactory(storeRestoreFactory2));
  }

  @Test
  public void testGetBackupManagers() {
    String storeName = "store1";
    String storeName2 = "store2";
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(FACTORY, storeName), "store1.factory.class");
    configMap.put(String.format(FACTORY, storeName2), "store2.factory.class");

    // empty config, return no restore managers
    assertEquals(Collections.emptySet(), new StorageConfig(new MapConfig(configMap)).getBackupFactories());
    assertEquals(Collections.emptyList(), new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName));
    assertEquals(Collections.emptyList(), new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName2));

    // changelog set, should default to kafka state backend restore
    String changelogStreamOverride = "changelogStream";
    configMap.put(String.format(CHANGELOG_STREAM, storeName), changelogStreamOverride);
    configMap.put(String.format(CHANGELOG_STREAM, storeName2), changelogStreamOverride);
    configMap.put(StorageConfig.CHANGELOG_SYSTEM, "changelog-system");
    configMap.put(String.format(StorageConfig.CHANGELOG_STREAM, storeName), "changelog-stream0");
    assertEquals(ImmutableSet.of(KAFKA_STATE_BACKEND_FACTORY), new StorageConfig(new MapConfig(configMap)).getBackupFactories());
    assertEquals(DEFAULT_BACKUP_FACTORIES, new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName));
    assertEquals(DEFAULT_BACKUP_FACTORIES, new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName2));
    assertEquals(ImmutableSet.of(storeName2, storeName),
        ImmutableSet.copyOf(new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(KAFKA_STATE_BACKEND_FACTORY)));

    // job restore manager config set should override to job backend factory
    String jobBackupFactory1 = "jobBackendBackupFactory1";
    String jobBackupFactory2 = "jobBackendBackupFactory2";
    String jobBackupFactoryOverride = jobBackupFactory1 + "," + jobBackupFactory2;
    configMap.put(JOB_BACKUP_FACTORIES, jobBackupFactoryOverride);
    assertEquals(ImmutableSet.of(jobBackupFactory1, jobBackupFactory2),
        new StorageConfig(new MapConfig(configMap)).getBackupFactories());
    assertEquals(ImmutableSet.of(jobBackupFactory1, jobBackupFactory2),
        ImmutableSet.copyOf(new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName)));
    assertEquals(ImmutableSet.of(jobBackupFactory1, jobBackupFactory2),
        ImmutableSet.copyOf(new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName2)));
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(KAFKA_STATE_BACKEND_FACTORY));
    assertEquals(ImmutableSet.of(storeName2, storeName),
        ImmutableSet.copyOf(new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(jobBackupFactory1)));
    assertEquals(ImmutableSet.of(storeName2, storeName),
        ImmutableSet.copyOf(new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(jobBackupFactory2)));

    // store specific restore managers set
    String storeBackupFactory1 = "storeBackendBackupFactory1";
    String storeBackupFactory2 = "storeBackendBackupFactory2";
    String storeBackupFactoryOverride = storeBackupFactory1 + "," + storeBackupFactory2;
    configMap.put(String.format(STORE_BACKUP_FACTORIES, storeName), storeBackupFactoryOverride);
    assertEquals(ImmutableSet.of(jobBackupFactory1, jobBackupFactory2, storeBackupFactory1, storeBackupFactory2),
        new StorageConfig(new MapConfig(configMap)).getBackupFactories());
    assertEquals(ImmutableList.of(storeBackupFactory1, storeBackupFactory2),
        new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName));
    assertEquals(ImmutableList.of(jobBackupFactory1, jobBackupFactory2),
        new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName2));
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(KAFKA_STATE_BACKEND_FACTORY));
    assertEquals(ImmutableList.of(storeName2),
        new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(jobBackupFactory1));
    assertEquals(ImmutableList.of(storeName2),
        new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(jobBackupFactory2));
    assertEquals(ImmutableList.of(storeName),
        new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(storeBackupFactory1));
    assertEquals(ImmutableList.of(storeName),
        new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(storeBackupFactory2));

    String emptyBackupFactory = "";
    configMap.put(String.format(STORE_BACKUP_FACTORIES, storeName), emptyBackupFactory);
    assertEquals(ImmutableSet.of(jobBackupFactory1, jobBackupFactory2),
        new StorageConfig(new MapConfig(configMap)).getBackupFactories());
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName));
    assertEquals(ImmutableList.of(jobBackupFactory1, jobBackupFactory2),
        new StorageConfig(new MapConfig(configMap)).getStoreBackupFactories(storeName2));
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(KAFKA_STATE_BACKEND_FACTORY));
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(storeBackupFactory1));
    assertEquals(Collections.emptyList(),
        new StorageConfig(new MapConfig(configMap)).getStoresWithBackupFactory(storeBackupFactory2));
  }

  @Test
  public void testGetMaxManifestFileSize() {
    // empty config, return default size, which is 1GB
    assertEquals(DEFAULT_ROCKSDB_MAX_MANIFEST_FILE_SIZE_IN_BYTES,
        new StorageConfig(new MapConfig()).getDefaultMaxManifestFileSizeBytes());

    StorageConfig storageConfig = new StorageConfig(
        new MapConfig(ImmutableMap.of(String.format(DEFAULT_ROCKSDB_MAX_MANIFEST_FILE_SIZE), "1024")));
    assertEquals(1024, storageConfig.getDefaultMaxManifestFileSizeBytes());
  }
}
