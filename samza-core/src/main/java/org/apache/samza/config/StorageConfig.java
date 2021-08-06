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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.execution.StreamManager;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.StreamUtil;

import static com.google.common.base.Preconditions.*;


/**
 * Config helper methods related to storage.
 */
public class StorageConfig extends MapConfig {
  private static final String FACTORY_SUFFIX = ".factory";
  private static final String CHANGELOG_SUFFIX = ".changelog";
  private static final String SIDE_INPUT_PROCESSOR_FACTORY_SUFFIX = ".side.inputs.processor.factory";
  private static final String STORE_PREFIX = "stores.";

  public static final String FACTORY = STORE_PREFIX + "%s" + FACTORY_SUFFIX;
  public static final String KEY_SERDE = STORE_PREFIX + "%s.key.serde";
  public static final String MSG_SERDE = STORE_PREFIX + "%s.msg.serde";
  public static final String CHANGELOG_STREAM = STORE_PREFIX + "%s" + CHANGELOG_SUFFIX;
  public static final String ACCESSLOG_STREAM_SUFFIX = "access-log";
  // TODO: setting replication.factor seems not working as in KafkaConfig.
  public static final String CHANGELOG_REPLICATION_FACTOR = STORE_PREFIX + "%s.changelog.replication.factor";
  public static final String CHANGELOG_MAX_MSG_SIZE_BYTES = STORE_PREFIX + "%s.changelog.max.message.size.bytes";
  public static final int DEFAULT_CHANGELOG_MAX_MSG_SIZE_BYTES = 1048576;
  public static final String DISALLOW_LARGE_MESSAGES = STORE_PREFIX + "%s.disallow.large.messages";
  public static final boolean DEFAULT_DISALLOW_LARGE_MESSAGES = false;
  public static final String DROP_LARGE_MESSAGES = STORE_PREFIX + "%s.drop.large.messages";
  public static final boolean DEFAULT_DROP_LARGE_MESSAGES = false;
  // The log compaction lag time for transactional state change log
  public static final String MIN_COMPACTION_LAG_MS = "min.compaction.lag.ms";
  public static final String CHANGELOG_MIN_COMPACTION_LAG_MS = STORE_PREFIX + "%s.changelog." + MIN_COMPACTION_LAG_MS;
  public static final long DEFAULT_CHANGELOG_MIN_COMPACTION_LAG_MS = TimeUnit.HOURS.toMillis(4);

  public static final String INMEMORY_KV_STORAGE_ENGINE_FACTORY =
      "org.apache.samza.storage.kv.inmemory.InMemoryKeyValueStorageEngineFactory";
  public static final String KAFKA_STATE_BACKEND_FACTORY =
      "org.apache.samza.storage.KafkaChangelogStateBackendFactory";
  public static final List<String> DEFAULT_BACKUP_FACTORIES = ImmutableList.of(
      KAFKA_STATE_BACKEND_FACTORY);
  public static final String JOB_BACKUP_FACTORIES = STORE_PREFIX + "backup.factories";
  public static final String STORE_BACKUP_FACTORIES = STORE_PREFIX + "%s.backup.factories";
  public static final String RESTORE_FACTORIES_SUFFIX = "restore.factories";
  public static final String STORE_RESTORE_FACTORIES = STORE_PREFIX + "%s." + RESTORE_FACTORIES_SUFFIX;
  public static final String JOB_RESTORE_FACTORIES = STORE_PREFIX + RESTORE_FACTORIES_SUFFIX;
  public static final List<String> DEFAULT_RESTORE_FACTORIES = ImmutableList.of(KAFKA_STATE_BACKEND_FACTORY);

  static final String CHANGELOG_SYSTEM = "job.changelog.system";
  static final String CHANGELOG_DELETE_RETENTION_MS = STORE_PREFIX + "%s.changelog.delete.retention.ms";
  static final long DEFAULT_CHANGELOG_DELETE_RETENTION_MS = TimeUnit.DAYS.toMillis(1);
  static final String ACCESSLOG_SAMPLING_RATIO = STORE_PREFIX + "%s.accesslog.sampling.ratio";
  static final String ACCESSLOG_ENABLED = STORE_PREFIX + "%s.accesslog.enabled";
  static final int DEFAULT_ACCESSLOG_SAMPLING_RATIO = 50;
  static final String SIDE_INPUTS = STORE_PREFIX + "%s.side.inputs";
  static final String SIDE_INPUTS_PROCESSOR_FACTORY = STORE_PREFIX + "%s" + SIDE_INPUT_PROCESSOR_FACTORY_SUFFIX;
  static final String SIDE_INPUTS_PROCESSOR_SERIALIZED_INSTANCE =
      STORE_PREFIX + "%s.side.inputs.processor.serialized.instance";

  // Internal config to clean storeDirs of a store on container start. This is used to benchmark bootstrap performance.
  static final String CLEAN_LOGGED_STOREDIRS_ON_START = STORE_PREFIX + "%s.clean.on.container.start";

  public StorageConfig(Config config) {
    super(config);
  }

  public List<String> getStoreNames() {
    Config subConfig = subset(STORE_PREFIX, true);
    List<String> storeNames = new ArrayList<>();
    for (String key : subConfig.keySet()) {
      if (key.endsWith(SIDE_INPUT_PROCESSOR_FACTORY_SUFFIX)) {
        storeNames.add(key.substring(0, key.length() - SIDE_INPUT_PROCESSOR_FACTORY_SUFFIX.length()));
      } else if (key.endsWith(FACTORY_SUFFIX)) {
        storeNames.add(key.substring(0, key.length() - FACTORY_SUFFIX.length()));
      }
    }
    return storeNames;
  }

  public Map<String, SystemStream> getStoreChangelogs() {
    return getStoreNames().stream().filter(store -> getChangelogStream(store).isPresent())
        .collect(Collectors.toMap(Function.identity(), n -> StreamUtil.getSystemStreamFromNames(getChangelogStream(n).get())));
  }

  /**
   * If the config specifies 'stores.&lt;storename&gt;.changelog' as '&lt;system&gt;.&lt;stream&gt;' combination - it will take
   * precedence.
   * If this config only specifies &lt;astream&gt; and there is a value in job.changelog.system=&lt;asystem&gt; - these values will
   * be combined into &lt;asystem&gt;.&lt;astream&gt;
   */
  public Optional<String> getChangelogStream(String storeName) {
    String systemStream = StringUtils.trimToNull(get(String.format(CHANGELOG_STREAM, storeName), null));

    String systemStreamRes;
    if (systemStream != null && !systemStream.contains(".")) {
      Optional<String> changelogSystem = getChangelogSystem();
      // contains only stream name
      if (changelogSystem.isPresent()) {
        systemStreamRes = changelogSystem.get() + "." + systemStream;
      } else {
        throw new SamzaException("changelog system is not defined:" + systemStream);
      }
    } else {
      systemStreamRes = systemStream;
    }

    if (systemStreamRes != null) {
      systemStreamRes = StreamManager.createUniqueNameForBatch(systemStreamRes, this);
    }
    return Optional.ofNullable(systemStreamRes);
  }

  public boolean getAccessLogEnabled(String storeName) {
    return getBoolean(String.format(ACCESSLOG_ENABLED, storeName), false);
  }

  public String getAccessLogStream(String changeLogStream) {
    return String.format("%s-%s", changeLogStream, ACCESSLOG_STREAM_SUFFIX);
  }

  public int getAccessLogSamplingRatio(String storeName) {
    return getInt(String.format(ACCESSLOG_SAMPLING_RATIO, storeName), DEFAULT_ACCESSLOG_SAMPLING_RATIO);
  }

  public Optional<String> getStorageFactoryClassName(String storeName) {
    return Optional.ofNullable(get(String.format(FACTORY, storeName)));
  }

  public Optional<String> getStorageKeySerde(String storeName) {
    return Optional.ofNullable(get(String.format(KEY_SERDE, storeName)));
  }

  public Optional<String> getStorageMsgSerde(String storeName) {
    return Optional.ofNullable(get(String.format(MSG_SERDE, storeName)));
  }

  /**
   * Gets the System to use for changelogs. Uses the following precedence.
   *
   * 1. If job.changelog.system is defined, that value is used.
   * 2. If job.default.system is defined, that value is used.
   * 3. empty optional
   *
   * Note: Changelogs can be defined using
   * stores.storeName.changelog=systemName.streamName  or
   * stores.storeName.changelog=streamName
   *
   * If the former syntax is used, that system name will still be honored. For the latter syntax, this method is used.
   *
   * @return the name of the system to use by default for all changelogs, if defined.
   */
  private Optional<String> getChangelogSystem() {
    return Optional.ofNullable(get(CHANGELOG_SYSTEM, get(JobConfig.JOB_DEFAULT_SYSTEM)));
  }

  /**
   * Gets the configured default for stores' changelog min.compaction.lag.ms, or if not defined uses the default
   * value defined in this class.
   *
   * @return the default changelog min.compaction.lag.ms
   */
  private long getDefaultChangelogMinCompactionLagMs() {
    String defaultMinCompactLagConfigName = STORE_PREFIX + "default.changelog." + MIN_COMPACTION_LAG_MS;
    return getLong(defaultMinCompactLagConfigName, DEFAULT_CHANGELOG_MIN_COMPACTION_LAG_MS);
  }

  /**
   * Gets the side inputs for the store. A store can have multiple side input streams which can be
   * provided as a comma separated list.
   *
   * Each side input must either be a {@code streamId}, or of the format {@code systemName.streamName}.
   * E.g. {@code stores.storeName.side.inputs = kafka.topicA, mySystem.topicB}
   *
   * @param storeName name of the store
   * @return a list of side input streams for the store, or an empty list if it has none.
   */
  public List<String> getSideInputs(String storeName) {
    return Optional.ofNullable(get(String.format(SIDE_INPUTS, storeName), null))
        .map(inputs -> Stream.of(inputs.split(","))
            .map(String::trim)
            .filter(input -> !input.isEmpty())
            .collect(Collectors.toList()))
        .orElse(Collections.emptyList());
  }

  /**
   * Gets the SideInputsProcessorFactory associated with the {@code storeName}.
   *
   * @param storeName name of the store
   * @return the class name of SideInputsProcessorFactory if present, empty optional otherwise
   */
  public Optional<String> getSideInputsProcessorFactory(String storeName) {
    return Optional.ofNullable(get(String.format(SIDE_INPUTS_PROCESSOR_FACTORY, storeName)));
  }

  /**
   * Gets the serialized instance of SideInputsProcessor associated with the {@code storeName}.
   *
   * @param storeName name of the store
   * @return the serialized instance of SideInputsProcessor if present, empty optional otherwise
   */
  public Optional<String> getSideInputsProcessorSerializedInstance(String storeName) {
    return Optional.ofNullable(get(String.format(SIDE_INPUTS_PROCESSOR_SERIALIZED_INSTANCE, storeName)));
  }

  public long getChangeLogDeleteRetentionInMs(String storeName) {
    return getLong(String.format(CHANGELOG_DELETE_RETENTION_MS, storeName), DEFAULT_CHANGELOG_DELETE_RETENTION_MS);
  }

  public int getChangelogMaxMsgSizeBytes(String storeName) {
    return getInt(String.format(CHANGELOG_MAX_MSG_SIZE_BYTES, storeName), DEFAULT_CHANGELOG_MAX_MSG_SIZE_BYTES);
  }

  public boolean getDisallowLargeMessages(String storeName) {
    return getBoolean(String.format(DISALLOW_LARGE_MESSAGES, storeName), DEFAULT_DISALLOW_LARGE_MESSAGES);
  }

  public boolean getDropLargeMessages(String storeName) {
    return getBoolean(String.format(DROP_LARGE_MESSAGES, storeName), DEFAULT_DROP_LARGE_MESSAGES);
  }

  public long getChangelogMinCompactionLagMs(String storeName) {
    String minCompactLagConfigName = String.format(CHANGELOG_MIN_COMPACTION_LAG_MS, storeName);
    // Avoid the inconsistency of overriding using stores.x.changelog.kafka...
    checkArgument(get("stores." + storeName + ".changelog.kafka." + MIN_COMPACTION_LAG_MS) == null,
        "Use " + minCompactLagConfigName + " to set kafka min.compaction.lag.ms property.");

    return getLong(minCompactLagConfigName, getDefaultChangelogMinCompactionLagMs());
  }

  /**
   * Helper method to check if there is any stores configured w/ a changelog
   */
  public boolean hasDurableStores() {
    Config subConfig = subset(STORE_PREFIX, true);
    return subConfig.keySet().stream().anyMatch(key -> key.endsWith(CHANGELOG_SUFFIX));
  }

  /**
   * Helper method to get the number of persistent stores.
   */
  public int getNumPersistentStores() {
    return (int) getStoreNames().stream()
        .map(storeName -> getStorageFactoryClassName(storeName))
        .filter(factoryName -> factoryName.isPresent())
        .filter(factoryName -> !factoryName.get().equals(INMEMORY_KV_STORAGE_ENGINE_FACTORY))
        .count();
  }

  private List<String> getJobStoreBackupFactories() {
    return getList(JOB_BACKUP_FACTORIES, new ArrayList<>());
  }

  /**
   * Backup state backend factory follows the precedence:
   *
   * 1. If stores.store-name.backup.factories config key exists the store-name, that value is used
   * 2. If stores.backup.factories is set for the job, that value is used
   * 3. If stores.store-name.changelog is set for store-name, the default Kafka changelog state backend factory
   * 4. Otherwise no backup factories will be configured for the store
   *
   * Note: that 2 takes precedence over 3 enables job based migration off of Changelog restores
   * @return List of backup factories for the store in order of backup precedence
   */
  public List<String> getStoreBackupFactories(String storeName) {
    List<String> storeBackupManagers;
    if (containsKey(String.format(STORE_BACKUP_FACTORIES, storeName))) {
      storeBackupManagers = getList(String.format(STORE_BACKUP_FACTORIES, storeName), new ArrayList<>());
    } else {
      storeBackupManagers = getJobStoreBackupFactories();
      // For backwards compatibility if the changelog is enabled, we use default kafka backup factory
      if (storeBackupManagers.isEmpty() && getChangelogStream(storeName).isPresent()) {
        storeBackupManagers = DEFAULT_BACKUP_FACTORIES;
      }
    }
    return storeBackupManagers;
  }

  public Set<String> getBackupFactories() {
    return getStoreNames().stream()
        .flatMap((storeName) -> getStoreBackupFactories(storeName).stream())
        .collect(Collectors.toSet());
  }

  public List<String> getStoresWithBackupFactory(String backendFactoryName) {
    return getStoreNames().stream()
        .filter((storeName) -> getStoreBackupFactories(storeName)
            .contains(backendFactoryName))
        .collect(Collectors.toList());
  }

  public List<String> getPersistentStoresWithBackupFactory(String backendFactoryName) {
    return getStoreNames().stream()
        .filter(storeName -> {
          Optional<String> storeFactory = getStorageFactoryClassName(storeName);
          return storeFactory.isPresent() &&
              !storeFactory.get().equals(StorageConfig.INMEMORY_KV_STORAGE_ENGINE_FACTORY);
        })
        .filter((storeName) -> getStoreBackupFactories(storeName)
            .contains(backendFactoryName))
        .collect(Collectors.toList());
  }

  private List<String> getJobStoreRestoreFactories() {
    return getList(JOB_RESTORE_FACTORIES, new ArrayList<>());
  }

  /**
   * Restore state backend factory follows the precedence:
   *
   * 1. If stores.store-name.restore.factories config key exists for the store-name, that value is used
   * 2. If stores.restore.factories is set for the job, that value is used
   * 3. If stores.store-name.changelog is set for store-name, the default Kafka changelog state backend factory
   * 4. Otherwise no restore factories will be configured for the store
   *
   * Note that 2 takes precedence over 3 enables job based migration off of Changelog restores
   * @return List of restore factories for the store in order of restoration precedence
   */
  public List<String> getStoreRestoreFactories(String storeName) {
    List<String> storeRestoreManagers;
    if (containsKey(String.format(STORE_RESTORE_FACTORIES, storeName))) {
      storeRestoreManagers = getList(String.format(STORE_RESTORE_FACTORIES, storeName), new ArrayList<>());
    } else {
      storeRestoreManagers = getJobStoreRestoreFactories();
      // for backwards compatibility if changelog is enabled, we use default Kafka backup factory
      if (storeRestoreManagers.isEmpty() && getChangelogStream(storeName).isPresent()) {
        storeRestoreManagers = DEFAULT_RESTORE_FACTORIES;
      }
    }
    return storeRestoreManagers;
  }

  public Set<String> getRestoreFactories() {
    return getStoreNames().stream()
        .flatMap((storesName) -> getStoreRestoreFactories(storesName).stream())
        .collect(Collectors.toSet());
  }

  public List<String> getStoresWithRestoreFactory(String backendFactoryName) {
    return getStoreNames().stream()
        .filter((storeName) -> getStoreRestoreFactories(storeName).contains(backendFactoryName))
        .collect(Collectors.toList());
  }

  /**
   * Helper method to get if logged store dirs should be deleted regardless of their contents.
   */
  public boolean cleanLoggedStoreDirsOnStart(String storeName) {
    return getBoolean(String.format(CLEAN_LOGGED_STOREDIRS_ON_START, storeName), false);
  }
}
