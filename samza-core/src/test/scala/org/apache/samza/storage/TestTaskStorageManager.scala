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

import org.apache.samza.Partition
import org.apache.samza.config.{MapConfig, StorageConfig}
import org.apache.samza.container.TaskName
import org.apache.samza.storage.StoreProperties.StorePropertiesBuilder
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.system._
import org.apache.samza.util.{FileUtil, SystemClock}
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class TestTaskStorageManager extends MockitoSugar {

  val store = "store1"
  val loggedStore = "loggedStore1"
  val taskName = new TaskName("testTask")

  @Before
  def setupTestDirs() {
    TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultStoreBaseDir, store , taskName)
                      .mkdirs()
    TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName)
                      .mkdirs()
  }

  @After
  def tearDownTestDirs() {
    FileUtil.rm(TaskStorageManagerBuilder.defaultStoreBaseDir)
    FileUtil.rm(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir)
  }

  /**
   * This tests the entire TaskStorageManager lifecycle for a Persisted Logged Store
   * For example, a RocksDb store with changelog needs to continuously update the offset file on flush & stop
   * When the task is restarted, it should restore correctly from the offset in the OFFSET file on disk (if available)
   */
  @Test
  def testStoreLifecycleForLoggedPersistedStore(): Unit = {
    // Basic test setup of SystemStream, SystemStreamPartition for this task
    val ss = new SystemStream("kafka", "testStream")
    val partition = new Partition(0)
    val ssp = new SystemStreamPartition(ss, partition)
    val storeDirectory = TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName)
    val storeFile = new File(storeDirectory, "store.sst")
    val offsetFile = new File(storeDirectory, "OFFSET")

    val mockStorageEngine: StorageEngine = createMockStorageEngine(isLoggedStore = true, isPersistedStore = true, storeFile)

    // Mock for StreamMetadataCache, SystemConsumer, SystemAdmin
    val mockStreamMetadataCache = mock[StreamMetadataCache]
    val mockSSPMetadataCache = mock[SSPMetadataCache]
    val mockSystemConsumer = mock[SystemConsumer]
    val mockSystemAdmin = mock[SystemAdmin]
    val changelogSpec = StreamSpec.createChangeLogStreamSpec("testStream", "kafka", 1)
    doNothing().when(mockSystemAdmin).validateStream(changelogSpec)
    doNothing().when(mockSystemConsumer).stop()

    // Test 1: Initial invocation - No store on disk (only changelog has data)
    // Setup initial sspMetadata
    var sspMetadata = new SystemStreamPartitionMetadata("0", "50", "51")
    var metadata = new SystemStreamMetadata("testStream", new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))
    when(mockSSPMetadataCache.getMetadata(ssp)).thenReturn(sspMetadata)

    val taskManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore, mockStorageEngine, mockSystemConsumer)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .setSSPMetadataCache(mockSSPMetadataCache)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .build

    taskManager.init

    assertTrue(storeFile.exists())
    assertFalse(offsetFile.exists())
    verify(mockSystemConsumer).register(ssp, "0")

    // Test 2: flush should update the offset file
    taskManager.flush()
    assertTrue(offsetFile.exists())
    assertEquals("50", FileUtil.readWithChecksum(offsetFile))

    // Test 3: Update sspMetadata before shutdown and verify that offset file is updated correctly
    when(mockSSPMetadataCache.getMetadata(ssp)).thenReturn(new SystemStreamPartitionMetadata("0", "100", "101"))
    taskManager.stop()
    assertTrue(storeFile.exists())
    assertTrue(offsetFile.exists())
    assertEquals("100", FileUtil.readWithChecksum(offsetFile))

    // Test 4: Initialize again with an updated sspMetadata; Verify that it restores from the correct offset
    sspMetadata = new SystemStreamPartitionMetadata("0", "150", "151")
    metadata = new SystemStreamMetadata("testStream", new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))
    when(mockSSPMetadataCache.getMetadata(ssp)).thenReturn(sspMetadata)
    when(mockSystemAdmin.getOffsetsAfter(Map(ssp -> "100").asJava)).thenReturn(Map(ssp -> "101").asJava)

    taskManager.init

    assertTrue(storeFile.exists())
    assertTrue(offsetFile.exists())
    verify(mockSystemConsumer).register(ssp, "101")
  }

  /**
   * This tests the entire TaskStorageManager lifecycle for an InMemory Logged Store
   * For example, an InMemory KV store with changelog should not update the offset file on flush & stop
   * When the task is restarted, it should ALWAYS restore correctly from the earliest offset
   */
  @Test
  def testStoreLifecycleForLoggedInMemoryStore(): Unit = {
    // Basic test setup of SystemStream, SystemStreamPartition for this task
    val ss = new SystemStream("kafka", "testStream")
    val partition = new Partition(0)
    val ssp = new SystemStreamPartition(ss, partition)
    val storeDirectory = TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultStoreBaseDir, store, taskName)

    val mockStorageEngine: StorageEngine = createMockStorageEngine(isLoggedStore = true, isPersistedStore = false, null)

    // Mock for StreamMetadataCache, SystemConsumer, SystemAdmin
    val mockStreamMetadataCache = mock[StreamMetadataCache]
    val mockSystemAdmin = mock[SystemAdmin]
    val changelogSpec = StreamSpec.createChangeLogStreamSpec("testStream", "kafka", 1)
    doNothing().when(mockSystemAdmin).validateStream(changelogSpec)

    val mockSystemConsumer = mock[SystemConsumer]
    doNothing().when(mockSystemConsumer).stop()

    // Test 1: Initial invocation - No store data (only changelog has data)
    // Setup initial sspMetadata
    val sspMetadata = new SystemStreamPartitionMetadata("0", "50", "51")
    var metadata = new SystemStreamMetadata("testStream", new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, sspMetadata)
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))
    val taskManager = new TaskStorageManagerBuilder()
      .addStore(store, mockStorageEngine, mockSystemConsumer)
      .setStreamMetadataCache(mockStreamMetadataCache)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .build

    taskManager.init

    // Verify that the store directory doesn't have ANY files
    assertNull(storeDirectory.listFiles())
    verify(mockSystemConsumer).register(ssp, "0")

    // Test 2: flush should NOT create/update the offset file. Store directory has no files
    taskManager.flush()
    assertNull(storeDirectory.listFiles())

    // Test 3: Update sspMetadata before shutdown and verify that offset file is NOT created
    metadata = new SystemStreamMetadata("testStream", new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, new SystemStreamPartitionMetadata("0", "100", "101"))
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))
    taskManager.stop()
    assertNull(storeDirectory.listFiles())

    // Test 4: Initialize again with an updated sspMetadata; Verify that it restores from the earliest offset
    metadata = new SystemStreamMetadata("testStream", new java.util.HashMap[Partition, SystemStreamPartitionMetadata]() {
      {
        put(partition, new SystemStreamPartitionMetadata("0", "150", "151"))
      }
    })
    when(mockStreamMetadataCache.getStreamMetadata(any(), any())).thenReturn(Map(ss -> metadata))

    taskManager.init

    assertNull(storeDirectory.listFiles())
    // second time to register; make sure it starts from beginning
    verify(mockSystemConsumer, times(2)).register(ssp, "0")
  }

  @Test
  def testStoreDirsWithoutOffsetFileAreDeletedInCleanBaseDirs() {
    val checkFilePath1 = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultStoreBaseDir, store, taskName), "check")
    checkFilePath1.createNewFile()
    val checkFilePath2 = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName), "check")
    checkFilePath2.createNewFile()

    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(store, false)
      .addStore(loggedStore, true)
      .build

    //Invoke test method
    val cleanDirMethod = taskStorageManager
                          .getClass
                          .getDeclaredMethod("cleanBaseDirs",
                                             new Array[java.lang.Class[_]](0):_*)
    cleanDirMethod.setAccessible(true)
    cleanDirMethod.invoke(taskStorageManager, new Array[Object](0):_*)

    assertTrue("check file was found in store partition directory. Clean up failed!", !checkFilePath1.exists())
    assertTrue("check file was found in logged store partition directory. Clean up failed!", !checkFilePath2.exists())
  }

  @Test
  def testLoggedStoreDirsWithOffsetFileAreNotDeletedInCleanBaseDirs() {
    val offsetFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName), "OFFSET")
    FileUtil.writeWithChecksum(offsetFilePath, "100")

    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore, true)
      .build

    val cleanDirMethod = taskStorageManager.getClass.getDeclaredMethod("cleanBaseDirs",
      new Array[java.lang.Class[_]](0):_*)
    cleanDirMethod.setAccessible(true)
    cleanDirMethod.invoke(taskStorageManager, new Array[Object](0):_*)

    assertTrue("Offset file was removed. Clean up failed!", offsetFilePath.exists())
    assertEquals("Offset read does not match what was in the file", "100", taskStorageManager.fileOffsets.get(new SystemStreamPartition("kafka", "testStream", new Partition(0))))
  }

  @Test
  def testStoreDeletedWhenOffsetFileOlderThanDeleteRetention() {
    // This test ensures that store gets deleted when lastModifiedTime of the offset file
    // is older than deletionRetention of the changeLog.
    val storeDirectory = TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName)
    val offsetFile = new File(storeDirectory, "OFFSET")
    offsetFile.createNewFile()
    FileUtil.writeWithChecksum(offsetFile, "Test Offset Data")
    offsetFile.setLastModified(0)
    val taskStorageManager = new TaskStorageManagerBuilder().addStore(store, false)
      .addStore(loggedStore, true)
      .build

    val cleanDirMethod = taskStorageManager.getClass
      .getDeclaredMethod("cleanBaseDirs",
        new Array[java.lang.Class[_]](0):_*)
    cleanDirMethod.setAccessible(true)
    cleanDirMethod.invoke(taskStorageManager, new Array[Object](0):_*)

    assertTrue("Offset file was found in store partition directory. Clean up failed!", !offsetFile.exists())
    assertTrue("Store directory exists. Clean up failed!", !storeDirectory.exists())
  }

  @Test
  def testOffsetFileIsRemovedInCleanBaseDirsForInMemoryLoggedStore() {
    val offsetFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName), "OFFSET")
    FileUtil.writeWithChecksum(offsetFilePath, "100")

    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore, false)
      .build

    val cleanDirMethod = taskStorageManager.getClass.getDeclaredMethod("cleanBaseDirs",
      new Array[java.lang.Class[_]](0):_*)
    cleanDirMethod.setAccessible(true)
    cleanDirMethod.invoke(taskStorageManager, new Array[Object](0):_*)

    assertFalse("Offset file was not removed. Clean up failed!", offsetFilePath.exists())
  }

  @Test
  def testStopCreatesOffsetFileForLoggedStore() {
    val partition = new Partition(0)

    val offsetFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName) + File.separator + "OFFSET")

    val sspMetadataCache = mock[SSPMetadataCache]
    val sspMetadata = new SystemStreamPartitionMetadata("20", "100", "101")
    when(sspMetadataCache.getMetadata(new SystemStreamPartition("kafka", "testStream", partition)))
      .thenReturn(sspMetadata)

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore, true)
      .setSSPMetadataCache(sspMetadataCache)
      .setPartition(partition)
      .build

    //Invoke test method
    taskStorageManager.stop()

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    assertEquals("Found incorrect value in offset file!", "100", FileUtil.readWithChecksum(offsetFilePath))
  }

  /**
    * Given that the SSPMetadataCache returns metadata, flush should create the offset files.
    */
  @Test
  def testFlushCreatesOffsetFileForLoggedStore() {
    val partition = new Partition(0)

    val offsetFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName) + File.separator + "OFFSET")
    val anotherOffsetPath = new File(
      TaskStorageManager.getStorePartitionDir(
        TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, store, taskName) + File.separator + "OFFSET")

    val sspMetadataCache = mock[SSPMetadataCache]
    val sspMetadata = new SystemStreamPartitionMetadata("20", "100", "101")
    when(sspMetadataCache.getMetadata(new SystemStreamPartition("kafka", "testStream", partition)))
      .thenReturn(sspMetadata)

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
            .addStore(loggedStore, true)
            .addStore(store, false)
            .setSSPMetadataCache(sspMetadataCache)
            .setPartition(partition)
            .build

    //Invoke test method
    taskStorageManager.flush()

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    assertEquals("Found incorrect value in offset file!", "100", FileUtil.readWithChecksum(offsetFilePath))

    assertTrue("Offset file got created for a store that is not persisted to the disk!!", !anotherOffsetPath.exists())
  }

  /**
    * Flush should delete the existing OFFSET file if the changelog partition (for some reason) becomes empty
    */
  @Test
  def testFlushDeletesOffsetFileForLoggedStoreForEmptyPartition() {
    val partition = new Partition(0)

    val offsetFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName) + File.separator + "OFFSET")

    val sspMetadataCache = mock[SSPMetadataCache]
    when(sspMetadataCache.getMetadata(new SystemStreamPartition("kafka", "testStream", partition)))
      // first return some metadata
      .thenReturn(new SystemStreamPartitionMetadata("0", "100", "101"))
      // then return no metadata to trigger the delete
      .thenReturn(null)

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
            .addStore(loggedStore, true)
            .setSSPMetadataCache(sspMetadataCache)
            .setPartition(partition)
            .build

    //Invoke test method
    taskStorageManager.flush()

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    assertEquals("Found incorrect value in offset file!", "100", FileUtil.readWithChecksum(offsetFilePath))

    //Invoke test method again
    taskStorageManager.flush()

    //Check conditions
    assertFalse("Offset file for null offset exists!", offsetFilePath.exists())
  }

  @Test
  def testFlushOverwritesOffsetFileForLoggedStore() {
    val partition = new Partition(0)
    val ssp = new SystemStreamPartition("kafka", "testStream", partition)

    val offsetFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName) + File.separator + "OFFSET")
    FileUtil.writeWithChecksum(offsetFilePath, "100")

    val sspMetadataCache = mock[SSPMetadataCache]
    when(sspMetadataCache.getMetadata(ssp)).thenReturn(new SystemStreamPartitionMetadata("20", "139", "140"))

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
            .addStore(loggedStore, true)
            .setSSPMetadataCache(sspMetadataCache)
            .setPartition(partition)
            .build

    //Invoke test method
    taskStorageManager.flush()

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    assertEquals("Found incorrect value in offset file!", "139", FileUtil.readWithChecksum(offsetFilePath))

    // Flush again
    when(sspMetadataCache.getMetadata(ssp)).thenReturn(new SystemStreamPartitionMetadata("20", "193", "194"))

    //Invoke test method
    taskStorageManager.flush()

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    assertEquals("Found incorrect value in offset file!", "193", FileUtil.readWithChecksum(offsetFilePath))
  }

  @Test
  def testStopShouldNotCreateOffsetFileForEmptyStore() {
    val partition = new Partition(0)

    val offsetFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName) + File.separator + "OFFSET")

    val sspMetadataCache = mock[SSPMetadataCache]
    when(sspMetadataCache.getMetadata(new SystemStreamPartition("kafka", "testStream", partition))).thenReturn(null)

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore, true)
      .setSSPMetadataCache(sspMetadataCache)
      .setPartition(partition)
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

  private def testChangelogConsumerOffsetRegistration(oldestOffset: String, newestOffset: String, upcomingOffset: String, expectedRegisteredOffset: String, fileOffset: String, writeOffsetFile: Boolean): Unit = {
    val systemName = "kafka"
    val streamName = "testStream"
    val partitionCount = 1
    // Basic test setup of SystemStream, SystemStreamPartition for this task
    val ss = new SystemStream(systemName, streamName)
    val partition = new Partition(0)
    val ssp = new SystemStreamPartition(ss, partition)
    val storeDirectory = TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName)
    val storeFile = new File(storeDirectory, "store.sst")

    if (writeOffsetFile) {
      val offsetFile = new File(storeDirectory, "OFFSET")
      if (fileOffset != null) {
        FileUtil.writeWithChecksum(offsetFile, fileOffset)
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
    when(mockSystemConsumer.register(any(), any())).thenAnswer(new Answer[Unit] {
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
      .build

    taskManager.init

    verify(mockSystemConsumer).register(any(classOf[SystemStreamPartition]), anyString())
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

object TaskStorageManagerBuilder {
  val defaultStoreBaseDir =  new File(System.getProperty("java.io.tmpdir") + File.separator + "store")
  val defaultLoggedStoreBaseDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "loggedStore")
}

class TaskStorageManagerBuilder extends MockitoSugar {
  var taskStores: Map[String, StorageEngine] = Map()
  var storeConsumers: Map[String, SystemConsumer] = Map()
  var changeLogSystemStreams: Map[String, SystemStream] = Map()
  var streamMetadataCache = mock[StreamMetadataCache]
  var sspMetadataCache = mock[SSPMetadataCache]
  var partition: Partition = new Partition(0)
  var systemAdmins: Map[String, SystemAdmin] = Map("kafka" -> mock[SystemAdmin])
  var taskName: TaskName = new TaskName("testTask")
  var storeBaseDir: File = TaskStorageManagerBuilder.defaultStoreBaseDir
  var loggedStoreBaseDir: File =  TaskStorageManagerBuilder.defaultLoggedStoreBaseDir
  var changeLogStreamPartitions: Int = 1

  def addStore(storeName: String, storageEngine: StorageEngine, systemConsumer: SystemConsumer): TaskStorageManagerBuilder = {
    taskStores = taskStores ++ Map(storeName -> storageEngine)
    storeConsumers = storeConsumers ++ Map(storeName -> systemConsumer)
    changeLogSystemStreams = changeLogSystemStreams ++ Map(storeName -> new SystemStream("kafka", "testStream"))
    this
  }

  def addStore(storeName: String, isPersistedToDisk: Boolean): TaskStorageManagerBuilder = {
    val mockStorageEngine = mock[StorageEngine]
    when(mockStorageEngine.getStoreProperties)
      .thenReturn(new StorePropertiesBuilder().setPersistedToDisk(isPersistedToDisk).setLoggedStore(false).build())
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
    systemAdmins = systemAdmins ++ Map(system -> systemAdmin)
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

  def setSSPMetadataCache(cache: SSPMetadataCache) = {
    sspMetadataCache = cache
    this
  }

  def build: TaskStorageManager = {
    new TaskStorageManager(
      taskName = taskName,
      taskStores = taskStores,
      storeConsumers = storeConsumers,
      changeLogSystemStreams = changeLogSystemStreams,
      changeLogStreamPartitions = changeLogStreamPartitions,
      streamMetadataCache = streamMetadataCache,
      sspMetadataCache = sspMetadataCache,
      nonLoggedStoreBaseDir = storeBaseDir,
      loggedStoreBaseDir = loggedStoreBaseDir,
      partition = partition,
      systemAdmins = new SystemAdmins(systemAdmins.asJava),
      new StorageConfig(new MapConfig()).getChangeLogDeleteRetentionsInMs,
      SystemClock.instance
    )
  }
}
