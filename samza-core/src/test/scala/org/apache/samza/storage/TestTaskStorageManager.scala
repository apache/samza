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


import java.io.File
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Matchers._
import scala.collection.JavaConversions

import org.apache.samza.container.TaskName
import org.apache.samza.util.Util
import org.apache.samza.system._
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.apache.samza.Partition

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
    Util.rm(TaskStorageManagerBuilder.defaultStoreBaseDir)
    Util.rm(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir)
  }

  @Test
  def testCleanBaseDirs() {
    val checkFilePath1 = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultStoreBaseDir, store, taskName), "check")
    checkFilePath1.createNewFile()
    val checkFilePath2 = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName), "check")
    checkFilePath2.createNewFile()

    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(store)
      .addStore(loggedStore)
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
  def testCleanBaseDirsWithOffsetFileForLoggedStore() {
    val checkFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName), "OFFSET")
    Util.writeDataToFile(checkFilePath, "100")

    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore)
      .build

    val cleanDirMethod = taskStorageManager.getClass.getDeclaredMethod("cleanBaseDirs",
      new Array[java.lang.Class[_]](0):_*)
    cleanDirMethod.setAccessible(true)
    cleanDirMethod.invoke(taskStorageManager, new Array[Object](0):_*)

    assertTrue("Offset file was not removed. Clean up failed!", !checkFilePath.exists())
    assertEquals("Offset read does not match what was in the file", "100", taskStorageManager.fileOffset.get(new SystemStreamPartition("kafka", "testStream", new Partition(0))))
  }

  @Test
  def testStopCreatesOffsetFileForLoggedStore() {
    val partition = new Partition(0)

    val offsetFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName) + File.separator + "OFFSET")

    val mockSystemAdmin = mock[SystemAdmin]
    val mockSspMetadata = Map("testStream" -> new SystemStreamMetadata("testStream" , JavaConversions.mapAsJavaMap[Partition, SystemStreamPartitionMetadata](Map(partition -> new SystemStreamPartitionMetadata("20", "100", "101")))))
    val myMap = JavaConversions.mapAsJavaMap[String, SystemStreamMetadata](mockSspMetadata)
    when(mockSystemAdmin.getSystemStreamMetadata(any(JavaConversions.setAsJavaSet(Set("")).getClass))).thenReturn(myMap)

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .setPartition(partition)
      .build

    //Invoke test method
    val stopMethod = taskStorageManager.getClass.getDeclaredMethod("stop", new Array[java.lang.Class[_]](0):_*)
    stopMethod.setAccessible(true)
    stopMethod.invoke(taskStorageManager, new Array[Object](0):_*)

    //Check conditions
    assertTrue("Offset file doesn't exist!", offsetFilePath.exists())
    assertEquals("Found incorrect value in offset file!", "100", Util.readDataFromFile(offsetFilePath))
  }

  @Test
  def testStopShouldNotCreateOffsetFileForEmptyStore() {
    val partition = new Partition(0)

    val offsetFilePath = new File(TaskStorageManager.getStorePartitionDir(TaskStorageManagerBuilder.defaultLoggedStoreBaseDir, loggedStore, taskName) + File.separator + "OFFSET")

    val mockSystemAdmin = mock[SystemAdmin]
    val mockSspMetadata = Map("testStream" -> new SystemStreamMetadata("testStream" , JavaConversions.mapAsJavaMap[Partition, SystemStreamPartitionMetadata](Map(partition -> new SystemStreamPartitionMetadata("20", null, null)))))
    val myMap = JavaConversions.mapAsJavaMap[String, SystemStreamMetadata](mockSspMetadata)
    when(mockSystemAdmin.getSystemStreamMetadata(any(JavaConversions.setAsJavaSet(Set("")).getClass))).thenReturn(myMap)

    //Build TaskStorageManager
    val taskStorageManager = new TaskStorageManagerBuilder()
      .addStore(loggedStore)
      .setSystemAdmin("kafka", mockSystemAdmin)
      .setPartition(partition)
      .build

    //Invoke test method
    val stopMethod = taskStorageManager.getClass.getDeclaredMethod("stop", new Array[java.lang.Class[_]](0):_*)
    stopMethod.setAccessible(true)
    stopMethod.invoke(taskStorageManager, new Array[Object](0):_*)

    //Check conditions
    assertTrue("Offset file should not exist!", !offsetFilePath.exists())
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
  val streamMetadataCache = mock[StreamMetadataCache]
  var partition: Partition = new Partition(0)
  var systemAdmins: Map[String, SystemAdmin] = Map("kafka" -> mock[SystemAdmin])
  var taskName: TaskName = new TaskName("testTask")
  var storeBaseDir: File = TaskStorageManagerBuilder.defaultStoreBaseDir
  var loggedStoreBaseDir: File =  TaskStorageManagerBuilder.defaultLoggedStoreBaseDir
  var changeLogStreamPartitions: Int = 1

  def addStore(storeName: String): TaskStorageManagerBuilder =  {
    taskStores = taskStores ++ Map(storeName -> mock[StorageEngine])
    storeConsumers = storeConsumers ++ Map(storeName -> mock[SystemConsumer])
    changeLogSystemStreams = changeLogSystemStreams ++ Map(storeName -> new SystemStream("kafka", "testStream"))
    this
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

  def build: TaskStorageManager = {
    new TaskStorageManager(
      taskName = taskName,
      taskStores = taskStores,
      storeConsumers = storeConsumers,
      changeLogSystemStreams = changeLogSystemStreams,
      changeLogStreamPartitions = changeLogStreamPartitions,
      streamMetadataCache = streamMetadataCache,
      storeBaseDir = storeBaseDir,
      loggedStoreBaseDir = loggedStoreBaseDir,
      partition = partition,
      systemAdmins = systemAdmins
    )
  }
}