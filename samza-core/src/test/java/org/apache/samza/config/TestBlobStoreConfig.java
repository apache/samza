package org.apache.samza.config;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.config.BlobStoreConfig.*;
import static org.apache.samza.config.BlobStoreConfig.CHANGELOG_STREAM;
import static org.apache.samza.config.StorageConfig.*;
import static org.junit.Assert.*;


public class TestBlobStoreConfig {
  private static final String STORE_NAME0 = "store0";
  private static final String STORE_NAME1 = "store1";
  private static final String STORE_NAME2 = "store2";
  private static final String STORE_NAME3 = "store3";

  @Test
  public void testGetBackupManagerFactories() {
    String factory1 = "factory1";
    String factory2 = "factory2";
    String factory3 = "factory3";
    Config config = new MapConfig(
      ImmutableMap.of(
          String.format(STORE_STATE_BACKEND_BACKUP_FACTORIES, STORE_NAME0), factory1 + "," + factory2,
          String.format(STORE_STATE_BACKEND_BACKUP_FACTORIES, STORE_NAME1), factory1,
          String.format(STORE_STATE_BACKEND_BACKUP_FACTORIES, STORE_NAME2), factory3,
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
    );
    BlobStoreConfig blobStoreConfig = new BlobStoreConfig(config);

    Set<String> factories = blobStoreConfig.getStateBackendBackupFactories(new StorageConfig(config).getStoreNames());
    assertTrue(factories.contains(factory1));
    assertTrue(factories.contains(factory2));
    assertTrue(factories.contains(factory3));
    assertTrue(factories.contains(DEFAULT_STORE_STATE_BACKEND_FACTORY));
    assertEquals(4, factories.size());
    Assert.assertEquals(ImmutableList.of(factory1, factory2), blobStoreConfig.getStoreBackupManagerClassName(STORE_NAME0));
    Assert.assertEquals(ImmutableList.of(factory1), blobStoreConfig.getStoreBackupManagerClassName(STORE_NAME1));
    Assert.assertEquals(ImmutableList.of(factory3), blobStoreConfig.getStoreBackupManagerClassName(STORE_NAME2));
    Assert.assertEquals(DEFAULT_STORE_STATE_BACKEND_BACKUP_FACTORIES, blobStoreConfig.getStoreBackupManagerClassName(STORE_NAME3));
    assertTrue(blobStoreConfig.getStoreBackupManagerClassName("emptyStore").isEmpty());
    assertTrue(blobStoreConfig.getStoreBackupManagerClassName("noFactoryStore").isEmpty());
  }

  @Test
  public void testGetStoreToBackup() {
    String targetFactory = "target.class";
    Config config = new MapConfig(
        ImmutableMap.of(
            String.format(BlobStoreConfig.STORE_STATE_BACKEND_BACKUP_FACTORIES, STORE_NAME0), targetFactory,
            String.format(BlobStoreConfig.STORE_STATE_BACKEND_BACKUP_FACTORIES, STORE_NAME1), targetFactory + "," +
                DEFAULT_STORE_STATE_BACKEND_FACTORY,
            String.format(BlobStoreConfig.STORE_STATE_BACKEND_BACKUP_FACTORIES, STORE_NAME2), DEFAULT_STORE_STATE_BACKEND_FACTORY),
        ImmutableMap.of(
            String.format(FACTORY, STORE_NAME0), "store0.factory.class",
            String.format(FACTORY, STORE_NAME1), "store1.factory.class",
            String.format(FACTORY, STORE_NAME2), "store2.factory.class",
            String.format(FACTORY, STORE_NAME3), "store3.factory.class",
            String.format(CHANGELOG_STREAM, STORE_NAME3), "nondefault-changelog-system.streamName"
        )
    );
    StorageConfig storageConfig = new StorageConfig(config);
    BlobStoreConfig blobStoreConfig = new BlobStoreConfig(config);

    List<String> targetStoreNames =
        blobStoreConfig.getStoresWithStateBackendBackupFactory(storageConfig.getStoreNames(), targetFactory);
    List<String> defaultStoreNames =
        blobStoreConfig.getStoresWithStateBackendBackupFactory(storageConfig.getStoreNames(), DEFAULT_STORE_STATE_BACKEND_FACTORY);
    assertTrue(targetStoreNames.containsAll(ImmutableList.of(STORE_NAME0, STORE_NAME1)));
    assertEquals(2, targetStoreNames.size());
    assertTrue(defaultStoreNames.containsAll(ImmutableList.of(STORE_NAME2, STORE_NAME1, STORE_NAME3)));
    assertEquals(3, defaultStoreNames.size());
  }

}
