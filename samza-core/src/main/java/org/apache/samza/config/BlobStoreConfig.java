package org.apache.samza.config;

/**
 * Config related helper methods for BlobStore.
 */
public class BlobStoreConfig extends MapConfig {

  public static final String BLOB_STORE_MANAGER_FACTORY = "blob.store.manager.factory";
  public static final String BLOB_STORE_ADMIN_FACTORY = "blob.store.admin.factory";
  public BlobStoreConfig(Config config) {
    super(config);
  }


  public String getBlobStoreManagerFactory() {
    // TODO BLOCKER dchen validate that if blob store state backend is configured for use this config is also set.
    return get(BLOB_STORE_MANAGER_FACTORY);
  }

  public String getBlobStoreAdminFactory() {
    return get(BLOB_STORE_ADMIN_FACTORY);
  }
}
