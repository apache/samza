package org.apache.samza.config;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * Config related helper methods for BlobStore.
 */
public class BlobStoreConfig extends MapConfig {
  private static final String STORE_PREFIX = "stores.";
  private static final String CHANGELOG_SUFFIX = ".changelog";

  public static final String DEFAULT_BLOB_STORE_STATE_BACKEND_FACTORY =
      "org.apache.samza.storage.blobstore.BlobStoreStateBackendFactory";
  public static final String BLOB_STORE_MANAGER_FACTORY = "blob.store.manager.factory";
  public static final String BLOB_STORE_STATE_BACKEND_ADMIN_FACTORY = "blob.store.state.backend.admin.factory";

  public static final String DEFAULT_STORE_STATE_BACKEND_FACTORY =
      "org.apache.samza.storage.KafkaChangelogStateBackendFactory";
  public static final List<String> DEFAULT_STORE_STATE_BACKEND_BACKUP_FACTORIES = ImmutableList.of(
      DEFAULT_STORE_STATE_BACKEND_FACTORY);
  public static final String STORE_STATE_BACKEND_BACKUP_FACTORIES = STORE_PREFIX + "%s.state.backend.backup.factories";
  public static final String STORE_STATE_BACKEND_RESTORE_FACTORY = STORE_PREFIX + "state.backend.restore.factory";

  public static final String CHANGELOG_STREAM = STORE_PREFIX + "%s" + CHANGELOG_SUFFIX;

  public BlobStoreConfig(Config config) {
    super(config);
  }

  public List<String> getStoreBackupManagerClassName(String storeName) {
    List<String> storeBackupManagers = getList(String.format(STORE_STATE_BACKEND_BACKUP_FACTORIES, storeName), new ArrayList<>());
    // For backwards compatibility if the changelog is enabled, we use default kafka backup factory
    if (storeBackupManagers.isEmpty() && hasChangelogStream(storeName)) {
      storeBackupManagers = DEFAULT_STORE_STATE_BACKEND_BACKUP_FACTORIES;
    }
    return storeBackupManagers;
  }

  public Set<String> getStateBackendBackupFactories(List<String> storeNames) {
    return storeNames.stream()
        .flatMap((storeName) -> getStoreBackupManagerClassName(storeName).stream())
        .collect(Collectors.toSet());
  }

  public List<String> getStoresWithStateBackendBackupFactory(List<String> storeNames, String backendFactoryName) {
    return storeNames.stream()
        .filter((storeName) -> getStoreBackupManagerClassName(storeName)
            .contains(backendFactoryName))
        .collect(Collectors.toList());
  }

  public String getStateBackendRestoreFactory() {
    return get(STORE_STATE_BACKEND_RESTORE_FACTORY, DEFAULT_STORE_STATE_BACKEND_FACTORY);
  }

  // TODO BLOCKER dchen update when making restore managers per store
  public List<String> getStoresWithRestoreFactory(List<String> storeNames, String backendFactoryName) {
    return storeNames.stream()
        .filter((storeName) -> getStateBackendRestoreFactory().equals(backendFactoryName))
        .collect(Collectors.toList());
  }

  public String getBlobStoreManagerFactory() {
    // TODO BLOCKER dchen validate that if blob store state backend is configured for use this config is also set.
    return get(BLOB_STORE_MANAGER_FACTORY);
  }

  public String getBlobStoreStateBackendAdminFactory() {
    return get(BLOB_STORE_STATE_BACKEND_ADMIN_FACTORY);
  }

  private boolean hasChangelogStream(String storeName) {
    String changelogStream = get(String.format(CHANGELOG_STREAM, storeName), null);
    return changelogStream != null;
  }
}
