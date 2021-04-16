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

package org.apache.samza.storage

import java.io.{File, FileOutputStream, ObjectOutputStream}
import java.util

import com.google.common.collect.{ImmutableMap, ImmutableSet}
import org.apache.samza.Partition
import org.apache.samza.checkpoint.{Checkpoint, CheckpointId, CheckpointManager}
import org.apache.samza.config._
import org.apache.samza.container.{SamzaContainerMetrics, TaskInstanceMetrics, TaskName}
import org.apache.samza.context.{ContainerContext, JobContext}
import org.apache.samza.job.model.{ContainerModel, TaskMode, TaskModel}
import org.apache.samza.serializers.{Serde, StringSerdeFactory}
import org.apache.samza.storage.StoreProperties.StorePropertiesBuilder
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system._
import org.apache.samza.task.TaskInstanceCollector
import org.apache.samza.util.{FileUtil, SystemClock}
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.{After, Before, Test}
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
  * This test is parameterized on the offsetFileName and is run for both
  * StorageManagerUtil.OFFSET_FILE_NAME_LEGACY and StorageManagerUtil.OFFSET_FILE_NAME_NEW.
  *
  * @param offsetFileName the name of the offset file.
  */
@RunWith(value = classOf[Parameterized])
class TestKafkaNonTransactionalStateTaskBackupManager(offsetFileName: String) extends MockitoSugar {

  val store = "store1"
  val loggedStore = "loggedStore1"
  val taskName = new TaskName("testTask")
  val storageManagerUtil = new StorageManagerUtil
  val fileUtil = new FileUtil

  @Before
  def setupTestDirs() {
    storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultStoreBaseDir, store, taskName, TaskMode.Active)
      .mkdirs()
    storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active)
      .mkdirs()
  }

  @After
  def tearDownTestDirs() {
    fileUtil.rm(TaskStorageManagerBuilder.defaultStoreBaseDir)
    fileUtil.rm(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir)
  }

  def getStreamName(storeName : String): String = {
    "testStream-"+storeName
  }

  /**
    * This tests the entire TaskStorageManager lifecycle for a Persisted Logged Store
    * For example, a RocksDb store with changelog needs to continuously update the offset file on flush & stop
    * When the task is restarted, it should restore correctly from the offset in the OFFSET file on disk (if available)
    */
  @Test
  def testStoreLifecycleForLoggedPersistedStore(): Unit = {
    // Basic test setup of SystemStream, SystemStreamPartition for this task
    val ss = new SystemStream("kafka", getStreamName(loggedStore))
    val partition = new Partition(0)
    val ssp = new SystemStreamPartition(ss, partition)
    val storeDirectory = storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir,
      loggedStore, taskName, TaskMode.Active)
    val storeFile = new File(storeDirectory, "store.sst")
    val offsetFile = new File(storeDirectory, offsetFileName)

    val mockStorageEngine: StorageEngine = createMockStorageEngine(isLoggedStore = true, isPersistedStore = true, storeFile)

    // Mock for StreamMetadataCache, SystemConsumer, SystemAdmin
    val mockStreamMetadataCache = mock[StreamMetadataCache]
    val mockSystemConsumer = mock[SystemConsumer]
    val mockSystemAdmin = mock[SystemAdmin]
    val changelogSpec = StreamSpec.createChangeLogStreamSpec(getStreamName(loggedStore), "kafka", 1)
    doNothing().when(mockSystemAdmin).validateStream(changelogSpec)
    doNothing().when(mockSystemConsumer).stop()

    // Test 1: Initial invocation - No store on disk (only changelog has data)
    // Setup initial sspMetadata
    var sspMetadata = new SystemStreamPartitionMetadata("0", "50", "51")
    var metadata = new SystemStreamMetadata(getStreamName(loggedStore), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata))

    var taskManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore, mockStorageEngine, mockSystemConsumer)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .initializeContainerStorageManager()
      .build

    assertTrue(storeFile.exists())
    assertFalse(offsetFile.exists())
    verify(mockSystemConsumer).register(ssp, "0")

    // Test 2: flush should update the offset file
    val checkpointId = CheckpointId.create()
    val snapshot = taskManager.snapshot(checkpointId)
    val stateCheckpointMarkers = taskManager.upload(checkpointId, snapshot)
    taskManager.persistToFilesystem(checkpointId, stateCheckpointMarkers.get())
    assertTrue(offsetFile.exists())
    validateOffsetFileContents(offsetFile, "kafka.testStream-loggedStore1.0", "50")

    // Test 3: Update sspMetadata before shutdown and verify that offset file is not updated
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp)))
      .thenReturn(ImmutableMap.of(ssp, new SystemStreamPartitionMetadata("0", "100", "101")))
    taskManager.stop()
    verify(mockStorageEngine, times(1)).flush() // only called once during Test 2.
    assertTrue(storeFile.exists())
    assertTrue(offsetFile.exists())
    validateOffsetFileContents(offsetFile, "kafka.testStream-loggedStore1.0", "50")

    // Test 4: Initialize again with an updated sspMetadata; Verify that it restores from the correct offset
    sspMetadata = new SystemStreamPartitionMetadata("0", "150", "151")
    metadata = new SystemStreamMetadata(getStreamName(loggedStore), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp)))
      .thenReturn(ImmutableMap.of(ssp, sspMetadata))
    when(mockSystemAdmin.getOffsetsAfter(Map(ssp -> "50").asJava)).thenReturn(Map(ssp -> "51").asJava)
    Mockito.reset(mockSystemConsumer)

    taskManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore, mockStorageEngine, mockSystemConsumer)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .initializeContainerStorageManager()
      .build

    assertTrue(storeFile.exists())
    assertTrue(offsetFile.exists())
    verify(mockSystemConsumer).register(ssp, "51")
  }

  /**
    * This tests the entire TaskStorageManager lifecycle for an InMemory Logged Store
    * For example, an InMemory KV store with changelog should not update the offset file on flush & stop
    * When the task is restarted, it should ALWAYS restore correctly from the earliest offset
    */
  @Test
  def testStoreLifecycleForLoggedInMemoryStore(): Unit = {
    // Basic test setup of SystemStream, SystemStreamPartition for this task
    val ss = new SystemStream("kafka", getStreamName(store))
    val partition = new Partition(0)
    val ssp = new SystemStreamPartition(ss, partition)
    val storeDirectory = storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, store, taskName, TaskMode.Active)

    val mockStorageEngine: StorageEngine = createMockStorageEngine(isLoggedStore = true, isPersistedStore = false, null)

    // Mock for StreamMetadataCache, SystemConsumer, SystemAdmin
    val mockStreamMetadataCache = mock[StreamMetadataCache]
    val mockSystemAdmin = mock[SystemAdmin]
    val changelogSpec = StreamSpec.createChangeLogStreamSpec(getStreamName(store), "kafka", 1)
    doNothing().when(mockSystemAdmin).validateStream(changelogSpec)

    val mockSystemConsumer = mock[SystemConsumer]
    doNothing().when(mockSystemConsumer).stop()

    // Test 1: Initial invocation - No store data (only changelog has data)
    // Setup initial sspMetadata
    val sspMetadata = new SystemStreamPartitionMetadata("0", "50", "51")
    var metadata = new SystemStreamMetadata(getStreamName(store), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata))
    var taskManager = new TaskStorageManagerBuilder()
      .addStore(store, mockStorageEngine, mockSystemConsumer)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .initializeContainerStorageManager()
      .build

    // Verify that the store directory doesn't have ANY files
    assertTrue(storeDirectory.list().isEmpty)
    verify(mockSystemConsumer).register(ssp, "0")

    // Test 2: flush should NOT create/update the offset file. Store directory has no files
    val checkpointId = CheckpointId.create()
    val snapshot = taskManager.snapshot(checkpointId)
    val stateCheckpointMarkers = taskManager.upload(checkpointId, snapshot)
    taskManager.persistToFilesystem(checkpointId, stateCheckpointMarkers.get())
    assertTrue(storeDirectory.list().isEmpty)

    // Test 3: Update sspMetadata before shutdown and verify that offset file is NOT created
    metadata = new SystemStreamMetadata(getStreamName(store), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, new SystemStreamPartitionMetadata("0", "100", "101"))
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata))
    taskManager.stop()
    assertTrue(storeDirectory.list().isEmpty)

    // Test 4: Initialize again with an updated sspMetadata; Verify that it restores from the earliest offset
    metadata = new SystemStreamMetadata(getStreamName(store), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, new SystemStreamPartitionMetadata("0", "150", "151"))
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))

    taskManager = new TaskStorageManagerBuilder()
      .addStore(store, mockStorageEngine, mockSystemConsumer)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .initializeContainerStorageManager()
      .build

    assertTrue(storeDirectory.list().isEmpty)
    // second time to register; make sure it starts from beginning
    verify(mockSystemConsumer, times(2)).register(ssp, "0")
  }

  @Test
  def testStoreDirsWithoutOffsetFileAreDeletedInCleanBaseDirs() {
    val checkFilePath1 = new File(storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultStoreBaseDir, store, taskName, TaskMode.Active), "check")
    checkFilePath1.createNewFile()
    val checkFilePath2 = new File(storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active), "check")
    checkFilePath2.createNewFile()

    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(store, false)
      .addLoggedStore(loggedStore, true)
      .setStreamMetadataCache(createMockStreamMetadataCache(null, null, null)) //empty store
      .initializeContainerStorageManager()
      .build

    assertTrue("check file was found in store partition directory. Clean up failed!", !checkFilePath1.exists())
    assertTrue("check file was found in logged store partition directory. Clean up failed!", !checkFilePath2.exists())
  }

  @Test
  def testLoggedStoreDirsWithOffsetFileAreNotDeletedInCleanBaseDirs() {
    val offsetFilePath = new File(storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active), offsetFileName)
    fileUtil.writeWithChecksum(offsetFilePath, "100")

    val taskStorageManager = new TaskStorageManagerBuilder()
      .addLoggedStore(loggedStore, true)
      .setStreamMetadataCache(createMockStreamMetadataCache(null, null, null)) // empty store
      .initializeContainerStorageManager()
      .build

    assertTrue("Offset file was removed. Clean up failed!", offsetFilePath.exists())
  }

  @Test
  def testStoreDeletedWhenOffsetFileOlderThanDeleteRetention() {
    // This test ensures that store gets deleted when lastModifiedTime of the offset file
    // is older than deletionRetention of the changeLog.
    val storeDirectory = storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active)
    storeDirectory.setLastModified(0)
    val offsetFile = new File(storeDirectory, offsetFileName)
    offsetFile.createNewFile()
    fileUtil.writeWithChecksum(offsetFile, "Test Offset Data")
    offsetFile.setLastModified(0)

    val taskStorageManager = new TaskStorageManagerBuilder().addStore(store, false)
      .addLoggedStore(loggedStore, true)
      .setStreamMetadataCache(createMockStreamMetadataCache("0", "1", "2"))
      .initializeContainerStorageManager()
      .build

    assertTrue("Offset file was found in store partition directory. Clean up failed!", !offsetFile.exists())
    assertTrue("Store directory should be deleted and re-created with new last modified time", storeDirectory.lastModified() > 0)
  }

  @Test
  def testStoreDeletedWhenCleanDirsFlagSet() {
    // This test ensures that store gets deleted when the stores.container.start.clean config is set,
    // and new dir is created with a new last modified time
    val storeDirectory = storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active)
    val offsetFile = new File(storeDirectory, offsetFileName)
    offsetFile.createNewFile()
    fileUtil.writeWithChecksum(offsetFile, "Test Offset Data")

    val taskStorageManager = new TaskStorageManagerBuilder().addStore(store, false)
      .addLoggedStore(loggedStore, true)
      .setStreamMetadataCache(createMockStreamMetadataCache("0", "1", "2"))
      .initializeContainerStorageManager(true)
      .build

    assertTrue("Offset file was found in store partition directory. Clean up failed!", !offsetFile.exists())
    assertTrue("Store directory should be deleted and re-created with new last modified time", storeDirectory.lastModified() > 0)
  }

  @Test
  def testOffsetFileIsRemovedInCleanBaseDirsForInMemoryLoggedStore() {
    val offsetFilePath = new File(storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active), offsetFileName)
    fileUtil.writeWithChecksum(offsetFilePath, "100")

    val taskStorageManager = new TaskStorageManagerBuilder()
      .addLoggedStore(loggedStore, false)
      .setStreamMetadataCache(createMockStreamMetadataCache(null, null, null)) // empty store
      .initializeContainerStorageManager()
      .build

    assertFalse("Offset file was not removed. Clean up failed!", offsetFilePath.exists())
  }

  @Test
  def testStopDoesNotCreatesOffsetFileForLoggedStore() {
    val partition = new Partition(0)

    val storeDirectory = storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active)
    val offsetFile = new File(storeDirectory, offsetFileName)

    val ssp = new SystemStreamPartition("kafka", getStreamName(loggedStore), partition)
    val mockSystemAdmin = mock[SystemAdmin]
    val sspMetadata = new SystemStreamPartitionMetadata("20", "100", "101")
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata))

    var metadata = new SystemStreamMetadata(getStreamName(loggedStore), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })

    val mockStreamMetadataCache = mock[StreamMetadataCache]
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(new SystemStream("kafka", getStreamName(loggedStore)) -> metadata))

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
      .addLoggedStore(loggedStore, true)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .setPartition(partition)
      .initializeContainerStorageManager()
      .build

    //Invoke test method
    taskStorageManager.stop()

    //Check conditions
    assertFalse("Offset file doesn't exist!", offsetFile.exists())
  }

  /**
    * Given that the SSPMetadataCache returns metadata, flush should create the offset files.
    */
  @Test
  def testFlushCreatesOffsetFileForLoggedStore() {
    val partition = new Partition(0)

    val offsetFilePath = new File(storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active) + File.separator + offsetFileName)
    val anotherOffsetPath = new File(
      storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, store, taskName, TaskMode.Active) + File.separator + offsetFileName)

    val ssp1 = new SystemStreamPartition("kafka", getStreamName(loggedStore), partition)
    val ssp2 = new SystemStreamPartition("kafka", getStreamName(store), partition)
    val sspMetadata = new SystemStreamPartitionMetadata("20", "100", "101")

    val mockSystemAdmin = mock[SystemAdmin]
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp1))).thenReturn(ImmutableMap.of(ssp1, sspMetadata))
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp2))).thenReturn(ImmutableMap.of(ssp2, sspMetadata))

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
      .addLoggedStore(loggedStore, true)
      .addStore(store, false)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .setStreamMetadataCache(createMockStreamMetadataCache("20", "100", "101"))
      .setPartition(partition)
      .initializeContainerStorageManager()
      .build

    //Invoke test method
    val checkpointId = CheckpointId.create()
    val snapshot = taskStorageManager.snapshot(checkpointId)
    val stateCheckpointMarkers = taskStorageManager.upload(checkpointId, snapshot)
    taskStorageManager.persistToFilesystem(checkpointId, stateCheckpointMarkers.get())

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    validateOffsetFileContents(offsetFilePath, "kafka.testStream-loggedStore1.0", "100")

    assertTrue("Offset file got created for a store that is not persisted to the disk!!", !anotherOffsetPath.exists())
  }

  /**
    * Flush should delete the existing OFFSET file if the changelog partition (for some reason) becomes empty
    */
  @Test
  def testFlushDeletesOffsetFileForLoggedStoreForEmptyPartition() {
    val partition = new Partition(0)

    val offsetFilePath = new File(storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active) + File.separator + offsetFileName)

    val ssp = new SystemStreamPartition("kafka", getStreamName(loggedStore), partition)
    val sspMetadata = new SystemStreamPartitionMetadata("0", "100", "101")
    val nullSspMetadata = new SystemStreamPartitionMetadata(null, null, null)
    val mockSystemAdmin = mock[SystemAdmin]
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp)))
      .thenReturn(ImmutableMap.of(ssp, sspMetadata))
      .thenReturn(ImmutableMap.of(ssp, nullSspMetadata))

    var metadata = new SystemStreamMetadata(getStreamName(loggedStore), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })

    val mockStreamMetadataCache = mock[StreamMetadataCache]
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(new SystemStream("kafka", getStreamName(loggedStore)) -> metadata))

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
      .addLoggedStore(loggedStore, true)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .setPartition(partition)
      .initializeContainerStorageManager()
      .build

    //Invoke test method
    val checkpointId = CheckpointId.create()
    var snapshot = taskStorageManager.snapshot(checkpointId)
    val stateCheckpointMarkers = taskStorageManager.upload(checkpointId, snapshot)
    taskStorageManager.persistToFilesystem(checkpointId, stateCheckpointMarkers.get())

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    validateOffsetFileContents(offsetFilePath, "kafka.testStream-loggedStore1.0", "100")

    //Invoke test method again
    snapshot = taskStorageManager.snapshot(checkpointId)
    val stateCheckpointMarkers2 = taskStorageManager.upload(checkpointId, snapshot)
    taskStorageManager.persistToFilesystem(checkpointId, stateCheckpointMarkers2.get())

    //Check conditions
    assertFalse("Offset file for null offset exists!", offsetFilePath.exists())
  }

  @Test
  def testFlushOverwritesOffsetFileForLoggedStore() {
    val partition = new Partition(0)
    val ssp = new SystemStreamPartition("kafka", getStreamName(loggedStore), partition)

    val offsetFilePath = new File(storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active) + File.separator + offsetFileName)
    fileUtil.writeWithChecksum(offsetFilePath, "100")

    val sspMetadata = new SystemStreamPartitionMetadata("20", "139", "140")
    val mockSystemAdmin = mock[SystemAdmin]
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp))).thenReturn(ImmutableMap.of(ssp, sspMetadata))


    var metadata = new SystemStreamMetadata(getStreamName(loggedStore), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })

    val mockStreamMetadataCache = mock[StreamMetadataCache]
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(new SystemStream("kafka", getStreamName(loggedStore)) -> metadata))

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
      .addLoggedStore(loggedStore, true)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .setPartition(partition)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .initializeContainerStorageManager()
      .build

    //Invoke test method
    val checkpointId = CheckpointId.create()
    var snapshot = taskStorageManager.snapshot(checkpointId)
    val stateCheckpointMarkers = taskStorageManager.upload(checkpointId, snapshot)
    taskStorageManager.persistToFilesystem(checkpointId, stateCheckpointMarkers.get())

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    validateOffsetFileContents(offsetFilePath, "kafka.testStream-loggedStore1.0", "139")

    // Flush again
    when(mockSystemAdmin.getSSPMetadata(ImmutableSet.of(ssp)))
      .thenReturn(ImmutableMap.of(ssp, new SystemStreamPartitionMetadata("20", "193", "194")))

    //Invoke test method
    snapshot = taskStorageManager.snapshot(checkpointId)
    val stateCheckpointMarkers2 = taskStorageManager.upload(checkpointId, snapshot)
    taskStorageManager.persistToFilesystem(checkpointId, stateCheckpointMarkers2.get())

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    validateOffsetFileContents(offsetFilePath, "kafka.testStream-loggedStore1.0", "193")
  }

  /**
    * Validates the contents of the offsetFile against the given ssp and offset.
    * The legacy offset file only contains the offset as a string, while the new offset file contains a map of
    * ssp to offset in json format.
    * The name of the two offset files are given in {@link StorageManagerUtil.OFFSET_FILE_NAME_NEW} and
    * {@link StorageManagerUtil.OFFSET_FILE_NAME_LEGACY}.
    */
  private def validateOffsetFileContents(offsetFile: File, ssp: String, offset: String): Unit = {

    if (offsetFile.getCanonicalFile.getName.equals(StorageManagerUtil.OFFSET_FILE_NAME_NEW)) {
      assertEquals("Found incorrect value in offset file!", "{\"" + ssp + "\":\"" + offset + "\"}", fileUtil.readWithChecksum(offsetFile))
    } else if (offsetFile.getCanonicalFile.getName.equals(StorageManagerUtil.OFFSET_FILE_NAME_LEGACY)) {
      assertEquals("Found incorrect value in offset file!", offset, fileUtil.readWithChecksum(offsetFile))
    } else {
      throw new IllegalArgumentException("Invalid offset file name");
    }
  }

  @Test
  def testStopShouldNotCreateOffsetFileForEmptyStore() {
    val partition = new Partition(0)

    val offsetFilePath = new File(storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active) + File.separator + offsetFileName)


    val sspMetadataCache = mock[SSPMetadataCache]
    when(sspMetadataCache.getMetadata(new SystemStreamPartition("kafka", getStreamName(loggedStore), partition))).thenReturn(null)

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
      .addLoggedStore(loggedStore, true)
      .setPartition(partition)
      .setStreamMetadataCache(createMockStreamMetadataCache(null, null, null)) // null offsets for empty store
      .initializeContainerStorageManager()
      .build

    //Invoke test method
    taskStorageManager.stop()

    //Check conditions
    assertTrue("Offset file should not exist!", !offsetFilePath.exists())
  }

  @Test
  def testCleanBaseDirsShouldNotAddNullOffsetsToFileOffsetsMap(): Unit = {
    // If a null file offset were allowed, and the full Map passed to SystemAdmin.getOffsetsAfter an NPE could
    // occur for some SystemAdmin implementations
    val writeOffsetFile = true
    val fileOffset = null
    val oldestOffset = "3"
    val newestOffset = "150"
    val upcomingOffset = "151"
    val expectedRegisteredOffset = "3"

    testChangelogConsumerOffsetRegistration(oldestOffset, newestOffset, upcomingOffset, expectedRegisteredOffset, fileOffset, writeOffsetFile)
  }

  @Test
  def testStartConsumersShouldRegisterCorrectOffsetWhenFileOffsetValid(): Unit = {
    // We should register the offset AFTER the stored file offset.
    // The file offset represents the last changelog message that is also reflected in the store. So start with next one.
    val writeOffsetFile = true
    val fileOffset = "139"
    val oldestOffset = "3"
    val newestOffset = "150"
    val upcomingOffset = "151"
    val expectedRegisteredOffset = "140"

    testChangelogConsumerOffsetRegistration(oldestOffset, newestOffset, upcomingOffset, expectedRegisteredOffset, fileOffset, writeOffsetFile)
  }

  @Test
  def testStartConsumersShouldRegisterCorrectOffsetWhenFileOffsetOlderThanOldestOffset(): Unit = {
    // We should register the oldest offset if it is less than the file offset
    val writeOffsetFile = true
    val fileOffset = "139"
    val oldestOffset = "145"
    val newestOffset = "150"
    val upcomingOffset = "151"
    val expectedRegisteredOffset = "145"

    testChangelogConsumerOffsetRegistration(oldestOffset, newestOffset, upcomingOffset, expectedRegisteredOffset, fileOffset, writeOffsetFile)
  }

  @Test
  def testStartConsumersShouldRegisterCorrectOffsetWhenOldestOffsetGreaterThanZero(): Unit = {
    val writeOffsetFile = false
    val fileOffset = null
    val oldestOffset = "3"
    val newestOffset = "150"
    val upcomingOffset = "151"
    val expectedRegisteredOffset = "3"

    testChangelogConsumerOffsetRegistration(oldestOffset, newestOffset, upcomingOffset, expectedRegisteredOffset, fileOffset, writeOffsetFile)
  }

  @Test
  def testReadOfOldOffsetFormat(): Unit = {
    // Create a file in old single-offset format, with a sample offset
    val storeDirectory = storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active)
    val storeFile = new File(storeDirectory, "store.sst")
    val offsetFile = new File(storeDirectory, offsetFileName)
    val sampleOldOffset = "912321"
    fileUtil.writeWithChecksum(offsetFile, sampleOldOffset)


    // read offset against a given ssp from the file
    var ssp = new SystemStreamPartition("kafka", "test-stream", new Partition(0))
    val offsets = storageManagerUtil.readOffsetFile(storeDirectory, Set(ssp).asJava, false)
    assertTrue(offsets.get(ssp).equals(sampleOldOffset))
  }

  @Test
  def testReadOfOffsetInCaseOfBothFilesPresent(): Unit = {
    // Create a file in old single-offset format, with a sample offset, and another with the new-offset format
    val storeDirectory = storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active)
    val storeFile = new File(storeDirectory, "store.sst")
    val sampleOldOffset = "100000001"
    val sampleNewOffset = "{\"kafka.test-stream.0\":\"200000002\"}"
    fileUtil.writeWithChecksum(new File(storeDirectory, StorageManagerUtil.OFFSET_FILE_NAME_LEGACY), sampleOldOffset)
    fileUtil.writeWithChecksum(new File(storeDirectory, StorageManagerUtil.OFFSET_FILE_NAME_NEW), sampleNewOffset)

    // Ensure that the files exist
    assertTrue(new File(storeDirectory, StorageManagerUtil.OFFSET_FILE_NAME_LEGACY).exists())
    assertTrue(new File(storeDirectory, StorageManagerUtil.OFFSET_FILE_NAME_NEW).exists())

    // read offset against a given ssp from the file, and check that the one in the new file should be read
    var ssp = new SystemStreamPartition("kafka", "test-stream", new Partition(0))
    val offsets = storageManagerUtil.readOffsetFile(storeDirectory, Set(ssp).asJava, false)

    assertEquals(1, offsets.size())
    assertEquals("200000002", offsets.get(ssp))
  }

  private def testChangelogConsumerOffsetRegistration(oldestOffset: String, newestOffset: String, upcomingOffset: String, expectedRegisteredOffset: String, fileOffset: String, writeOffsetFile: Boolean): Unit = {
    val systemName = "kafka"
    val streamName = getStreamName(loggedStore)
    val partitionCount = 1
    // Basic test setup of SystemStream, SystemStreamPartition for this task
    val ss = new SystemStream(systemName, streamName)
    val partition = new Partition(0)
    val ssp = new SystemStreamPartition(ss, partition)
    val storeDirectory = storageManagerUtil.getTaskStoreDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName, TaskMode.Active  )
    val storeFile = new File(storeDirectory, "store.sst")

    if (writeOffsetFile) {
      val offsetFile = new File(storeDirectory, offsetFileName)
      if (fileOffset != null) {
        fileUtil.writeWithChecksum(offsetFile, fileOffset)
      } else {
        // Write garbage to produce a null result when it's read
        val fos = new FileOutputStream(offsetFile)
        val oos = new ObjectOutputStream(fos)
        oos.writeLong(1)
        oos.writeUTF("Bad Offset")
        oos.close()
        fos.close()
      }
    }

    val mockStorageEngine: StorageEngine = createMockStorageEngine(isLoggedStore = true, isPersistedStore = true, storeFile)

    // Mock for StreamMetadataCache, SystemConsumer, SystemAdmin
    val mockStreamMetadataCache = mock[StreamMetadataCache]

    val mockSystemAdmin = mock[SystemAdmin]
    val changelogSpec = StreamSpec.createChangeLogStreamSpec(streamName, systemName, partitionCount)
    doNothing().when(mockSystemAdmin).validateStream(changelogSpec)
    when(mockSystemAdmin.getOffsetsAfter(any())).thenAnswer(new Answer[util.Map[SystemStreamPartition, String]] {
      override def answer(invocation: InvocationOnMock): util.Map[SystemStreamPartition, String] = {
        val originalOffsets = invocation.getArgumentAt(0, classOf[util.Map[SystemStreamPartition, String]])
        originalOffsets.asScala.mapValues(offset => (offset.toLong + 1).toString).asJava
      }
    })
    when(mockSystemAdmin.offsetComparator(any(), any())).thenAnswer(new Answer[Integer] {
      override def answer(invocation: InvocationOnMock): Integer = {
        val offset1 = invocation.getArgumentAt(0, classOf[String])
        val offset2 = invocation.getArgumentAt(1, classOf[String])
        offset1.toLong compare offset2.toLong
      }
    })

    val mockSystemConsumer = mock[SystemConsumer]
    when(mockSystemConsumer.register(any(classOf[SystemStreamPartition]), any(classOf[String]))).thenAnswer(new Answer[Unit] {
      override def answer(invocation: InvocationOnMock): Unit = {
        val args = invocation.getArguments
        if (ssp.equals(args.apply(0).asInstanceOf[SystemStreamPartition])) {
          val offset = args.apply(1).asInstanceOf[String]
          assertNotNull(offset)
          assertEquals(expectedRegisteredOffset, offset)
        }
      }
    })
    doNothing().when(mockSystemConsumer).stop()

    // Test 1: Initial invocation - No store on disk (only changelog has data)
    // Setup initial sspMetadata
    val sspMetadata = new SystemStreamPartitionMetadata(oldestOffset, newestOffset, upcomingOffset)
    var metadata = new SystemStreamMetadata(streamName, new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))
    when(mockSystemAdmin.getSystemStreamMetadata(any())).thenReturn(new util.HashMap[String, SystemStreamMetadata]() {
      {
        put(streamName, metadata)
      }
    })

    val taskManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore, mockStorageEngine, mockSystemConsumer)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .setSystemAdmin(systemName, mockSystemAdmin)
      .initializeContainerStorageManager()
      .build

    verify(mockSystemConsumer).register(any(classOf[SystemStreamPartition]), anyString())
  }

  private def createMockStreamMetadataCache(oldestOffset: String, newestOffset: String, upcomingOffset: String) = {
    // an empty store would return a SSPMetadata with oldest, newest and upcoming offset set to null
    var metadata1 = new SystemStreamMetadata(getStreamName(loggedStore), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(new Partition(0), new SystemStreamPartitionMetadata(oldestOffset, newestOffset, upcomingOffset))
      }
    })

    var metadata2 = new SystemStreamMetadata(getStreamName(store), new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(new Partition(0), new SystemStreamPartitionMetadata(oldestOffset, newestOffset, upcomingOffset))
      }
    })

    val mockStreamMetadataCache = mock[StreamMetadataCache]
    when(mockStreamMetadataCache.getStreamMetadata(org.mockito.Matchers.eq(Set(new SystemStream("kafka", getStreamName(loggedStore)))), any())).thenReturn(Map(new SystemStream("kafka", getStreamName(loggedStore)) -> metadata1))
    when(mockStreamMetadataCache.getStreamMetadata(org.mockito.Matchers.eq(Set(new SystemStream("kafka", getStreamName(store)))), any())).thenReturn(Map(new SystemStream("kafka", getStreamName(store)) -> metadata2))
    when(mockStreamMetadataCache.getStreamMetadata(org.mockito.Matchers.eq(Set(new SystemStream("kafka", getStreamName(store)), new SystemStream("kafka", getStreamName(loggedStore)))), any())).
      thenReturn(Map(new SystemStream("kafka", getStreamName(store)) -> metadata2, new SystemStream("kafka", getStreamName(loggedStore)) -> metadata1))

    mockStreamMetadataCache
  }

  private def createMockStorageEngine(isLoggedStore: Boolean, isPersistedStore: Boolean, storeFile: File) = {
    val mockStorageEngine = mock[StorageEngine]
    // getStoreProperties should always return the same StoreProperties
    when(mockStorageEngine.getStoreProperties).thenAnswer(new Answer[StoreProperties] {
      override def answer(invocation: InvocationOnMock): StoreProperties = {
        new StorePropertiesBuilder().setLoggedStore(isLoggedStore).setPersistedToDisk(isPersistedStore).build()
      }
    })
    // Restore simply creates the file
    if (storeFile != null) {
      when(mockStorageEngine.restore(any())).thenAnswer(new Answer[Unit] {
        override def answer(invocation: InvocationOnMock): Unit = {
          storeFile.createNewFile()
        }
      })
    } else {
      doNothing().when(mockStorageEngine).restore(any())
    }
    mockStorageEngine
  }
}

object TestKafkaNonTransactionalStateTaskBackupManager {

  @Parameters def parameters: util.Collection[Array[String]] = {
    val offsetFileNames = new util.ArrayList[Array[String]]()
    offsetFileNames.add(Array(StorageManagerUtil.OFFSET_FILE_NAME_NEW))
    offsetFileNames.add(Array(StorageManagerUtil.OFFSET_FILE_NAME_LEGACY))
    offsetFileNames
  }
}


object TaskStorageManagerBuilder {
  val defaultStoreBaseDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "store")
  val defaultLoggedStoreBaseDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "loggedStore")
}

class TaskStorageManagerBuilder extends MockitoSugar {
  var taskStores: Map[String, StorageEngine] = Map()
  var storeConsumers: Map[String, SystemConsumer] = Map()
  var changeLogSystemStreams: Map[String, SystemStream] = Map()
  var streamMetadataCache = mock[StreamMetadataCache]
  var partition: Partition = new Partition(0)
  var systemAdminsMap: Map[String, SystemAdmin] = Map("kafka" -> mock[SystemAdmin])
  var taskName: TaskName = new TaskName("testTask")
  var storeBaseDir: File = TaskStorageManagerBuilder.defaultStoreBaseDir
  var loggedStoreBaseDir: File = TaskStorageManagerBuilder.defaultLoggedStoreBaseDir
  var changeLogStreamPartitions: Int = 1
  var containerStorageManager: ContainerStorageManager = mock[ContainerStorageManager]

  def addStore(storeName: String, storageEngine: StorageEngine, systemConsumer: SystemConsumer): TaskStorageManagerBuilder = {
    taskStores = taskStores ++ Map(storeName -> storageEngine)
    storeConsumers = storeConsumers ++ Map("kafka" -> systemConsumer)
    changeLogSystemStreams = changeLogSystemStreams ++ Map(storeName -> new SystemStream("kafka", getStreamName(storeName)))
    this
  }

  def getStreamName(storeName : String): String = {
    "testStream-"+storeName
  }

  def addStore(storeName: String, isPersistedToDisk: Boolean): TaskStorageManagerBuilder = {
    val mockStorageEngine = mock[StorageEngine]
    when(mockStorageEngine.getStoreProperties)
      .thenReturn(new StorePropertiesBuilder().setPersistedToDisk(isPersistedToDisk).setLoggedStore(false).build())
    addStore(storeName, mockStorageEngine, mock[SystemConsumer])
  }

  def addLoggedStore(storeName: String, isPersistedToDisk: Boolean): TaskStorageManagerBuilder = {
    val mockStorageEngine = mock[StorageEngine]
    when(mockStorageEngine.getStoreProperties)
      .thenReturn(new StorePropertiesBuilder().setPersistedToDisk(isPersistedToDisk).setLoggedStore(true).build())
    addStore(storeName, mockStorageEngine, mock[SystemConsumer])
  }

  def setPartition(p: Partition) = {
    partition = p
    this
  }

  def setChangeLogSystemStreams(storeName: String, systemStream: SystemStream) = {
    changeLogSystemStreams = changeLogSystemStreams ++ Map(storeName -> systemStream)
    this
  }

  def setSystemAdmin(system: String, systemAdmin: SystemAdmin) = {
    systemAdminsMap = systemAdminsMap ++ Map(system -> systemAdmin)
    this
  }

  def setTaskName(tn: TaskName) = {
    taskName = tn
    this
  }

  def setStreamMetadataCache(metadataCache: StreamMetadataCache) = {
    streamMetadataCache = metadataCache
    this
  }

  /**
    * This method creates and starts a {@link ContainerStorageManager}
    */
  def initializeContainerStorageManager(cleanStoreDirsOnStart : Boolean = false) = {
    var tasks: Map[TaskName, TaskModel] = HashMap[TaskName, TaskModel]((taskName, new TaskModel(taskName, new util.HashSet[SystemStreamPartition], new Partition(0))))
    var containerModel = new ContainerModel("container", tasks.asJava)

    val mockSystemAdmins = Mockito.mock(classOf[SystemAdmins])
    Mockito.when(mockSystemAdmins.getSystemAdmin(org.mockito.Matchers.eq("kafka"))).thenReturn(systemAdminsMap.get("kafka").get)

    var mockStorageEngineFactory : StorageEngineFactory[AnyRef, AnyRef] = Mockito.mock(classOf[StorageEngineFactory[AnyRef, AnyRef]])

    var storageEngineFactories : mutable.Map[String, StorageEngineFactory[AnyRef, AnyRef]] =  scala.collection.mutable.Map[String, StorageEngineFactory[AnyRef, AnyRef]]()

    if(taskStores.contains("store1")) {
      Mockito.when(mockStorageEngineFactory.getStorageEngine(org.mockito.Matchers.eq("store1"), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(taskStores.get("store1").get)
      storageEngineFactories += ("store1" -> mockStorageEngineFactory)
    }

    if(taskStores.contains("loggedStore1")) {
      Mockito.when(mockStorageEngineFactory.getStorageEngine(org.mockito.Matchers.eq("loggedStore1"), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(taskStores.get("loggedStore1").get)
      storageEngineFactories += ("loggedStore1" -> mockStorageEngineFactory)
    }


    var mockSystemFactory = Mockito.mock(classOf[SystemFactory])
    Mockito.when(mockSystemFactory.getConsumer(org.mockito.Matchers.eq("kafka"),any(), any())).thenReturn(storeConsumers.get("kafka").get)
    var systemFactories : Map[String, SystemFactory] = HashMap[String, SystemFactory](("kafka", mockSystemFactory))

    var config =  new MapConfig(mutable.Map(
      "stores.store1.clean.on.container.start" -> cleanStoreDirsOnStart.toString,
      "stores.loggedStore1.clean.on.container.start" -> cleanStoreDirsOnStart.toString,
      "stores.store1.key.serde" -> classOf[StringSerdeFactory].getCanonicalName,
      "stores.store1.msg.serde" -> classOf[StringSerdeFactory].getCanonicalName,
      "stores.loggedStore1.key.serde" -> classOf[StringSerdeFactory].getCanonicalName,
      "stores.loggedStore1.msg.serde" -> classOf[StringSerdeFactory].getCanonicalName,
      TaskConfig.TRANSACTIONAL_STATE_RESTORE_ENABLED -> "false").asJava)

    var mockSerdes: Map[String, Serde[AnyRef]] = HashMap[String, Serde[AnyRef]]((classOf[StringSerdeFactory].getCanonicalName, Mockito.mock(classOf[Serde[AnyRef]])))

    val mockCheckpointManager = Mockito.mock(classOf[CheckpointManager])
    when(mockCheckpointManager.readLastCheckpoint(any(classOf[TaskName])))
      .thenReturn(new Checkpoint(new util.HashMap[SystemStreamPartition, String]()))

    val mockSSPMetadataCache = Mockito.mock(classOf[SSPMetadataCache])

    containerStorageManager = new ContainerStorageManager(
      mockCheckpointManager,
      containerModel,
      streamMetadataCache,
      mockSSPMetadataCache,
      mockSystemAdmins,
      changeLogSystemStreams.asJava,
      Map[String, util.Set[SystemStream]]().asJava,
      storageEngineFactories.asJava,
      systemFactories.asJava,
      mockSerdes.asJava,
      config,
      new HashMap[TaskName, TaskInstanceMetrics]().asJava,
      Mockito.mock(classOf[SamzaContainerMetrics]),
      Mockito.mock(classOf[JobContext]),
      Mockito.mock(classOf[ContainerContext]),
      new HashMap[TaskName, TaskInstanceCollector].asJava,
      loggedStoreBaseDir,
      TaskStorageManagerBuilder.defaultStoreBaseDir,
      1,
      null,
      new SystemClock)
    this
  }



  def build: KafkaNonTransactionalStateTaskBackupManager = {

    if (containerStorageManager != null) {
      containerStorageManager.start()
    }

    new KafkaNonTransactionalStateTaskBackupManager(
      taskName = taskName,
      taskStores = containerStorageManager.getAllStores(taskName),
      storeChangelogs = changeLogSystemStreams.asJava,
      systemAdmins = buildSystemAdmins(systemAdminsMap),
      loggedStoreBaseDir = loggedStoreBaseDir,
      partition = partition
    )
  }

  private def buildSystemAdmins(systemAdminsMap: Map[String, SystemAdmin]): SystemAdmins = {
    val systemAdmins = mock[SystemAdmins]
    systemAdminsMap.foreach { case (system, systemAdmin) =>
      when(systemAdmins.getSystemAdmin(system)).thenReturn(systemAdmin)
    }
    systemAdmins
  }
}
