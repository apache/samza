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

import java.io._
import java.util
import java.util.concurrent.TimeUnit

import org.apache.samza.config.{Config, StorageConfig, TaskConfigJava}
import org.apache.samza.{Partition, SamzaException}
import org.apache.samza.config.TaskConfig.Config2Task
import org.apache.samza.container.TaskName
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.processors.{SideInputProcessor, SideInputProcessorFactory}
import org.apache.samza.storage.kv.{Entry, KeyValueStore}
import org.apache.samza.system._
import org.apache.samza.util.{Clock, FileUtil, Logging, Util}
import org.codehaus.jackson.map.ObjectMapper

import scala.collection.mutable
import scala.collection.JavaConverters._

object TaskStorageManager {
  def getStoreDir(storeBaseDir: File, storeName: String) = {
    new File(storeBaseDir, storeName)
  }

  def getStorePartitionDir(storeBaseDir: File, storeName: String, taskName: TaskName) = {
    // TODO: Sanitize, check and clean taskName string as a valid value for a file
    new File(storeBaseDir, (storeName + File.separator + taskName.toString).replace(' ', '_'))
  }
}

/**
 * Manage all the storage engines for a given task
 */
class TaskStorageManager(
  taskName: TaskName,
  taskStores: Map[String, StorageEngine] = Map(),
  storeConsumers: Map[String, SystemConsumer] = Map(),
  changeLogSystemStreams: Map[String, SystemStream] = Map(),
  changeLogStreamPartitions: Int,
  streamMetadataCache: StreamMetadataCache,
  sspMetadataCache: SSPMetadataCache,
  nonLoggedStoreBaseDir: File = new File(System.getProperty("user.dir"), "state"),
  loggedStoreBaseDir: File = new File(System.getProperty("user.dir"), "state"),
  partition: Partition,
  systemAdmins: SystemAdmins,
  changeLogDeleteRetentionsInMs: Map[String, Long],
  clock: Clock,
  storesToSSPs: Map[String, Set[SystemStreamPartition]] = Map(),
  config: Config,
  metricsRegistry: MetricsRegistry) extends Logging {

  val loggedStoreOffsetFileName = "OFFSET"
  val sideInputOffsetFileName = "SIDE-INPUT-OFFSET"
  private val sideInputStoreDeleteRetentionInMs = TimeUnit.DAYS.toMillis(1)

  val persistedStores = taskStores.filter(_._2.getStoreProperties.isPersistedToDisk)

  // side input related fields
  private val checkpointSerde: ObjectMapper = new ObjectMapper()
  private val sideInputStores = taskStores.filter(_._2.getStoreProperties.hasSideInputs)
  private val sideInputProcessor: SideInputProcessor = {
    config.getSideInputProcessorFactory()
      .map(factoryClass => Util.getObj(factoryClass, classOf[SideInputProcessorFactory]))
      .map(factory => factory.createInstance(config, metricsRegistry))
      .getOrElse({
        if(taskStores.mapValues(_.getStoreProperties.hasSideInputs).count(_._2) > 0) {
          throw new SamzaException("Missing side input processor! Make sure your job has %s configured." format TaskConfigJava.SIDE_INPUT_PROCESSOR_FACTORY)
        }
        null
      })
  }
  private val sspsToStore = {
    val sspsToStoreMapping: mutable.Map[SystemStreamPartition, String] = mutable.Map()
    for((storeName, ssps) <- storesToSSPs) {
      ssps.foreach(ssp => sspsToStoreMapping.put(ssp, storeName))
    }

    sspsToStoreMapping
  }

  var oldestOffsets: mutable.Map[SystemStreamPartition, String] = mutable.Map[SystemStreamPartition, String]()
  var fileOffsets: util.Map[SystemStreamPartition, String] = new util.HashMap[SystemStreamPartition, String]()

  var lastProcessedOffsetsForSideInputSSPs: mutable.Map[SystemStreamPartition, String] = mutable.Map()
  var startingOffsetForSideInputSSPs: mutable.Map[SystemStreamPartition, String] = mutable.Map()

  var taskStoresToRestore = taskStores.filter(_._2.getStoreProperties.isLoggedStore)

  def apply(storageEngineName: String) = taskStores(storageEngineName)

  def init {
    cleanBaseDirs()
    setupBaseDirs()
    validateChangelogStreams()
    initializeOldestOffset()
    startConsumers()
    restoreStores()
    stopConsumers()
  }

  private def cleanBaseDirs() {
    debug("Cleaning base directories for stores.")

    taskStores.keys.foreach(storeName => {
      if (taskStoresToRestore.contains(storeName)) { // clean up base directories for logged stores
        val loggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
        info("Got logged storage partition directory as %s" format loggedStorePartitionDir.toPath.toString)

        // Delete the logged store if it is not valid.
        if (!isLoggedStoreValid(storeName, loggedStorePartitionDir)) {
          info("Deleting logged storage partition directory %s." format loggedStorePartitionDir.toPath.toString)
          FileUtil.rm(loggedStorePartitionDir)
        } else {
          val offset = readLoggedStoreOffset(loggedStorePartitionDir)
          info("Read offset %s for the store %s from logged storage partition directory %s." format(offset, storeName, loggedStorePartitionDir))
          if (offset != null) {
            fileOffsets.put(new SystemStreamPartition(changeLogSystemStreams(storeName), partition), offset)
          }
        }

      } else if (sideInputStores.contains(storeName)) { // clean up base directories for side input stores
        val sideInputStorePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
        info("Got side input storage partition directory as %s" format sideInputStorePartitionDir.toPath.toString)

        if (!isValidSideInputStore(storeName, sideInputStorePartitionDir)) {
          info("Deleting side input storage partition directory %s." format sideInputStorePartitionDir.toPath.toString)
        } else {
          val offsets: util.Map[SystemStreamPartition, String] = readSideInputStoreOffsets(sideInputStorePartitionDir)
          info("Read offset %s for the store %s from storage partition directory %s." format(offsets, storeName, sideInputStorePartitionDir))
          fileOffsets.putAll(offsets)
        }

      } else { // handle clean up of base directories for non-logged stores
        val nonLoggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(nonLoggedStoreBaseDir, storeName, taskName)
        info("Got non logged storage partition directory as %s" format nonLoggedStorePartitionDir.toPath.toString)

        if(nonLoggedStorePartitionDir.exists()) {
          info("Deleting non logged storage partition directory %s" format nonLoggedStorePartitionDir.toPath.toString)
          FileUtil.rm(nonLoggedStorePartitionDir)
        }
      }
    })
  }

  /**
    * Directory {@code loggedStoreDir} associated with the logged store {@code storeName} is valid,
    * if all of the following conditions are true.
    * a) If the store has to be persisted to disk.
    * b) If there is a valid offset file associated with the logged store.
    * c) If the logged store has not gone stale.
    *
    * @return true if the logged store is valid, false otherwise.
    */
  private def isLoggedStoreValid(storeName: String, loggedStoreDir: File): Boolean = {
    val changeLogDeleteRetentionInMs = changeLogDeleteRetentionsInMs.getOrElse(storeName,
                                                                               StorageConfig.DEFAULT_CHANGELOG_DELETE_RETENTION_MS)
    persistedStores.contains(storeName) && isOffsetFileValid(loggedStoreDir, loggedStoreOffsetFileName) &&
      !isStaleLoggedStore(loggedStoreDir, changeLogDeleteRetentionInMs)
  }

  /**
    * Directory {@code sideInputStorePartitionDir} associated with the store {@code storeName} is valid,
    * if all the following conditions are true.
    *  1. If the store has to be persisted to disk.
    *  2. If there is a valid offset file associated with the store.
    *  3. If the store has not gone stale.
    *
    * @param storeName store name
    * @param sideInputStorePartitionDir the base directory of the store
    *
    * @return true if the store is valid, false otherwise
    */
  private def isValidSideInputStore(storeName: String, sideInputStorePartitionDir: File): Boolean = {
    persistedStores.contains(storeName) && isOffsetFileValid(sideInputStorePartitionDir, sideInputOffsetFileName) &&
      !isStaleSideInputStore(sideInputStorePartitionDir)
  }

  /**
    * Determines if the logged store directory {@code loggedStoreDir} is stale. A store is stale if the following condition is true.
    *
    *  ((CurrentTime) - (LastModifiedTime of the Offset file) is greater than the changelog's tombstone retention).
    *
    * @param loggedStoreDir the base directory of the local change-logged store.
    * @param changeLogDeleteRetentionInMs the delete retention of the changelog in milli seconds.
    * @return true if the store is stale, false otherwise.
    *
    */
  private def isStaleLoggedStore(loggedStoreDir: File, changeLogDeleteRetentionInMs: Long): Boolean =
    isStaleStore(loggedStoreDir, loggedStoreOffsetFileName, changeLogDeleteRetentionInMs)

  /**
    * Checks if the store is stale. If the time elapsed since the last modified time of the offset file is greater than
    * {@code SIDE_INPUT_STORE_DEFAULT_DELETE_RETENTION_MS}, then the store is considered stale. For stale stores, we ignore the locally
    * checkpointed offsets and go with the oldest offset from the source.
    *
    * @param sideInputStoreLocation the base directory of the local side-input store.
    *
    * @return true if the store is stale, false otherwise
    */
  private def isStaleSideInputStore(sideInputStoreLocation: File): Boolean =
    isStaleStore(sideInputStoreLocation, sideInputOffsetFileName, sideInputStoreDeleteRetentionInMs)


  /**
    * Checks if the store is stale. IF the time elapsed since the last modified time of the offset file is greater than
    * the {@code storeDeleteRetentionInMs}, then the store is considered stale. For stale stores, we ignore the locally
    * checkpointed offsets and go with the oldest offset from the source.
    *
    * @param storeDir the base directory of the store
    * @param offsetFileName the offset file name
    * @param storeDeleteRetentionInMs store delete retention in millis
    *
    * @return true if the store is stale, false otherwise
    */
  private def isStaleStore(storeDir: File, offsetFileName: String, storeDeleteRetentionInMs: Long): Boolean = {
    var isStaleStore = false
    val storePath = storeDir.toPath.toString
    if (storeDir.exists()) {
      val offsetFileRef = new File(storeDir, offsetFileName)
      val offsetFileLastModifiedTime = offsetFileRef.lastModified()
      if ((clock.currentTimeMillis() - offsetFileLastModifiedTime) >= storeDeleteRetentionInMs) {
        info ("Store: %s is stale since lastModifiedTime of offset file: %s, " +
          "is older than store deleteRetentionMs: %s." format(storePath, offsetFileLastModifiedTime, storeDeleteRetentionInMs))
        isStaleStore = true
      }
    } else {
      info("Storage partition directory: %s does not exist." format storePath)
    }
    isStaleStore
  }

  /**
    * An offset file associated with logged store {@code storeDir} is valid if it exists and is not empty.
    *
    * @param storeDir the base directory of the store
    * @param offsetFileName name of the offset file
    *
    * @return true if the offset file is valid. false otherwise.
    */
  private def isOffsetFileValid(storeDir: File, offsetFileName: String): Boolean = {
    var hasValidOffsetFile = false
    if (storeDir.exists()) {
      val offsetContents = readOffsetFile(storeDir, offsetFileName)
      if (offsetContents != null && !offsetContents.isEmpty) {
        hasValidOffsetFile = true
      } else {
        info("Offset file is not valid for store: %s." format storeDir.toPath.toString)
      }
    }
    hasValidOffsetFile
  }

  private def setupBaseDirs() {
    debug("Setting up base directories for stores.")
    taskStores.foreach {
      case (storeName, storageEngine) =>
        if (storageEngine.getStoreProperties.isLoggedStore) {
          val loggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
          info("Using logged storage partition directory: %s for store: %s." format(loggedStorePartitionDir.toPath.toString, storeName))
          if (!loggedStorePartitionDir.exists()) loggedStorePartitionDir.mkdirs()
        } else if (sideInputStores.contains(storeName)) {
          val sideInputStorePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
          info("Using storage partition directory: %s for side input store: %s." format(sideInputStorePartitionDir.toPath.toString, storeName))
          if (!sideInputStorePartitionDir.exists()) sideInputStorePartitionDir.mkdirs()
        } else {
          val nonLoggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(nonLoggedStoreBaseDir, storeName, taskName)
          info("Using non logged storage partition directory: %s for store: %s." format(nonLoggedStorePartitionDir.toPath.toString, storeName))
          nonLoggedStorePartitionDir.mkdirs()
        }
    }
  }

  /**
    * Read and return the contents of the offset file.
    *
    * @param storagePartitionDir the base directory of the store
    * @param offsetFileName name of the offset file
    *
    * @return the content of the offset file if it exists for the store, null otherwise.
    */
  private def readOffsetFile(storagePartitionDir: File, offsetFileName: String): String = {
    var offset : String = null
    val offsetFileRef = new File(storagePartitionDir, offsetFileName)
    if (offsetFileRef.exists()) {
      info("Found offset file in storage partition directory: %s" format storagePartitionDir.toPath.toString)
      offset = FileUtil.readWithChecksum(offsetFileRef)
    } else {
      info("No offset file found in storage partition directory: %s" format storagePartitionDir.toPath.toString)
    }
    offset
  }

  /**
    * Reads and return the offset for the logged store
    *
    * @param loggedStoragePartitionDir the base directory of the logged store
    *
    * @return offset for the store if exists, null otherwise
    */
  private def readLoggedStoreOffset(loggedStoragePartitionDir: File): String = {
    readOffsetFile(loggedStoragePartitionDir, loggedStoreOffsetFileName)
  }

  /**
    * Loads the store offsets from the locally checkpointed file.
    * The offsets of the store are stored as tuples from [[SystemStreamPartition]] to [[String]] offset.
    *
    * <pre>
    *   SSP1 -> "offset1",
    *   SSP2 -> "offset2"
    * </pre>
    */
  private def readSideInputStoreOffsets(sideInputStoreLocation: File): util.Map[SystemStreamPartition, String] = {
    var offsets:util.Map[SystemStreamPartition, String] = null
    try {
      val checkpoint = readOffsetFile(sideInputStoreLocation, sideInputOffsetFileName)
      offsets = checkpointSerde.readValue(checkpoint, classOf[util.Map[SystemStreamPartition, String]])
    } catch {
      case e: Exception => warn("Failed to load the checkpoints from %s " format sideInputStoreLocation, e)
    }

    offsets
  }

  private def validateChangelogStreams() = {
    info("Validating change log streams: " + changeLogSystemStreams)

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val systemAdmin = systemAdmins.getSystemAdmin(systemStream.getSystem)
      val changelogSpec = StreamSpec.createChangeLogStreamSpec(systemStream.getStream, systemStream.getSystem, changeLogStreamPartitions)

      systemAdmin.validateStream(changelogSpec)
    }
  }

  private def initializeOldestOffset() = {

    // Load the oldest offset for logged stores
    val changeLogMetadata = streamMetadataCache.getStreamMetadata(changeLogSystemStreams.values.toSet)
    info("Got change log stream metadata: %s" format changeLogMetadata)

    val changeLogOldestOffsets: Map[SystemStream, String] = getChangeLogOldestOffsetsForPartition(partition, changeLogMetadata)
    info("Assigning oldest change log offsets for taskName %s: %s" format (taskName, changeLogOldestOffsets))

    val oldestOffsetForLoggedStores = changeLogOldestOffsets
      .map(entry => (new SystemStreamPartition(entry._1, partition), entry._2))

    // Load the oldest offsets for side input stores
    val oldestOffsetForSideInputStores = getSideInputStoresOldestOffsets()

    oldestOffsets ++= oldestOffsetForLoggedStores
    oldestOffsets ++= oldestOffsetForSideInputStores
  }

  private def startConsumers() {
    debug("Starting consumers for stores.")

    for ((storeName, systemStream) <- changeLogSystemStreams) {
      val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
      val consumer = storeConsumers(storeName)

      val offset = getLoggedStoreStartingOffset(systemStreamPartition)
      if (offset != null) {
        info("Registering change log consumer with offset %s for %s." format (offset, systemStreamPartition))
        consumer.register(systemStreamPartition, offset)
      } else {
        info("Skipping change log restoration for %s because stream appears to be empty (offset was null)." format systemStreamPartition)
        taskStoresToRestore -= storeName
      }
    }

    storeConsumers.values.foreach(_.start)
  }

  /**
    * Returns the offset with which the changelog consumer should be initialized for the given SystemStreamPartition.
    *
    * If a file offset exists, it represents the last changelog offset which is also reflected in the on-disk state.
    * In that case, we use the next offset after the file offset, as long as it is newer than the oldest offset
    * currently available in the stream.
    *
    * If there isn't a file offset or it's older than the oldest available offset, we simply start with the oldest.
    *
    * @param systemStreamPartition  the changelog partition for which the offset is needed.
    * @return                       the offset to from which the changelog consumer should be initialized.
    */
  private def getLoggedStoreStartingOffset(systemStreamPartition: SystemStreamPartition) = {
    val admin = systemAdmins.getSystemAdmin(systemStreamPartition.getSystem)
    val oldestOffset = oldestOffsets.getOrElse(systemStreamPartition,
        throw new SamzaException("Missing a change log offset for %s." format systemStreamPartition))

    getStartingOffset(systemStreamPartition, admin, oldestOffset)
  }

  private def getStartingOffset(ssp: SystemStreamPartition, admin: SystemAdmin, oldestOffset: String) = {
    val fileOffset = fileOffsets.get(ssp)

    if (fileOffset != null) {
      // File offset was the last message written to the local checkpoint that is also reflected in the store,
      // so we start with the NEXT offset
      val resumeOffset = admin.getOffsetsAfter(Map(ssp -> fileOffset).asJava).get(ssp)
      if (admin.offsetComparator(oldestOffset, resumeOffset) <= 0) {
        resumeOffset
      } else {
        // If the offset we plan to use is older than the oldest offset, just use the oldest offset.
        // This can happen with source of the store(changelog, etc) configured with a TTL cleanup policy
        warn(s"Local store offset $resumeOffset is lower than the oldest offset $oldestOffset of the source stream. " +
          s"The values between these offsets cannot be restored.")
        oldestOffset
      }
    } else {
      oldestOffset
    }
  }

  /**
    * Loads the oldest offset for the {@link SystemStreamPartition}s associated with all the stores.
    * It does multiple things to obtain the oldest offsets and the logic is as follows...
    *   1. Group the list of the SSPs for the side input storage manager based on system stream
    *   2. Fetch the system stream metadata from {@link StreamMetadataCache}
    *   3. Fetch the partition metadata for each system stream and fetch the corresponding partition metadata and populate
    *      the offset for SSPs belonging to the system stream.
    *
    * @return a [[Map]] of [[SystemStreamPartition]] to offset.
    */
  private def getSideInputStoresOldestOffsets(): Map[SystemStreamPartition, String] = {
    var oldestOffsets: mutable.Map[SystemStreamPartition, String] = mutable.Map()

    // Step 1
    val systemStreamToSSPs= storesToSSPs.values
      .flatMap(_.toStream)
      .groupBy(_.getSystemStream)

    // Step 2
    val metadata = streamMetadataCache.getStreamMetadata(systemStreamToSSPs.keySet, true)

    // Step 3
    for((systemStream, systemStreamMetadata) <- metadata) {
      val partitionMetadata = systemStreamMetadata.getSystemStreamPartitionMetadata

      // For SSPs belonging to the system stream, use the partition metadata to get the oldest offset
      val offsets: Map[SystemStreamPartition, String] = systemStreamToSSPs.getOrElse(systemStream, Set())
        .map(ssp => (ssp, partitionMetadata.get(ssp.getPartition).getOldestOffset)).toMap

      oldestOffsets ++=offsets
    }

    oldestOffsets.toMap
  }

  private def restoreStores() {
    debug("Restoring stores.")

    for ((storeName, store) <- taskStoresToRestore) {
      if (changeLogSystemStreams.contains(storeName)) {
        val systemStream = changeLogSystemStreams(storeName)
        val systemStreamPartition = new SystemStreamPartition(systemStream, partition)
        val systemConsumer = storeConsumers(storeName)
        val systemConsumerIterator = new SystemStreamPartitionIterator(systemConsumer, systemStreamPartition)
        store.restore(systemConsumerIterator)
      }
    }
  }

  private def stopConsumers() {
    debug("Stopping consumers for stores.")

    storeConsumers.values.foreach(_.stop)
  }

  def flush() {
    debug("Flushing stores.")

    taskStores.values.foreach(_.flush)
    flushChangelogOffsetFiles()
    flushSideInputStoreOffsets()
  }

  def stopStores() {
    debug("Stopping stores.")
    taskStores.values.foreach(_.stop)
  }

  def stop() {
    stopStores()

    flushChangelogOffsetFiles()
    flushSideInputStoreOffsets()
  }

  /**
    * Writes the offset files for each changelog to disk.
    * These files are used when stores are restored from disk to determine whether
    * there is any new information in the changelog that is not reflected in the disk
    * copy of the store. If there is any delta, it is replayed from the changelog
    * e.g. This can happen if the job was run on this host, then another
    * host and back to this host.
    */
  private def flushChangelogOffsetFiles() {
    debug("Persisting logged key value stores")

    for ((storeName, systemStream) <- changeLogSystemStreams.filterKeys(storeName => persistedStores.contains(storeName))) {
      debug("Fetching newest offset for store %s" format(storeName))
      try {
        val ssp = new SystemStreamPartition(systemStream.getSystem, systemStream.getStream, partition)
        val sspMetadata = sspMetadataCache.getMetadata(ssp)
        val newestOffset = if (sspMetadata == null) null else sspMetadata.getNewestOffset
        debug("Got offset %s for store %s" format(newestOffset, storeName))

        val loggedStorePartitionDir = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
        val offsetFile = new File(loggedStorePartitionDir, loggedStoreOffsetFileName)
        if (newestOffset != null) {
          debug("Storing offset for store in OFFSET file ")
          FileUtil.writeWithChecksum(offsetFile, newestOffset)
          debug("Successfully stored offset %s for store %s in OFFSET file " format(newestOffset, storeName))
        } else {
          //if newestOffset is null, then it means the store is (or has become) empty. No need to persist the offset file
          if (offsetFile.exists()) {
            FileUtil.rm(offsetFile)
          }
          debug("Not storing OFFSET file for taskName %s. Store %s backed by changelog topic: %s, partition: %s is empty. " format (taskName, storeName, systemStream.getStream, partition.getPartitionId))
        }
      } catch {
        case e: Exception => error("Exception storing offset for store %s. Skipping." format(storeName), e)
      }

    }

    debug("Done persisting logged key value stores")
  }

  /**
    * Flushes the offsets of all side input stores. One offset file is maintained per store and the format of the offsets
    * are as follows...
    * <pre>
    *   Offset file for SideInputStore1
    *
    *    SideInputStore1SSP1 --> offset1
    *    SideInputStore1SSP2 --> offset2
    *
    *   Offset file for SideInputStore2
    *
    *    SideInputStore2SSP1 --> offset1
    *    SideInputStore2SSP2 --> offset2
    *    SideInputStore2SSP3 --> offset3
    * </pre>
    *
    */
  private def flushSideInputStoreOffsets() {

    for ((storeName, ssps) <- storesToSSPs) {
      val offsets = ssps
        .filter(lastProcessedOffsetsForSideInputSSPs.contains(_))
        .map(ssp => (ssp, lastProcessedOffsetsForSideInputSSPs.get(ssp))).toMap

      try {
        val storeLocation = TaskStorageManager.getStorePartitionDir(loggedStoreBaseDir, storeName, taskName)
        val checkpoint = checkpointSerde.writeValueAsString(offsets)
        val offsetFile = new File(storeLocation, sideInputOffsetFileName)
        FileUtil.writeWithChecksum(offsetFile, checkpoint)
      } catch {
        case _: Exception => {
          error("Exception storing offset for the store %s. Skipping" format(storeName))
        }
      }
    }
  }

  /**
   * Builds a map from SystemStreamPartition to oldest offset for changelogs.
   */
  private def getChangeLogOldestOffsetsForPartition(partition: Partition, inputStreamMetadata: Map[SystemStream, SystemStreamMetadata]): Map[SystemStream, String] = {
    inputStreamMetadata
      .mapValues(_.getSystemStreamPartitionMetadata.get(partition))
      .filter(_._2 != null)
      .mapValues(_.getOldestOffset)
  }

  /**
    * Processes the incoming message envelope by fetching the associated store and updates the last processed offset.
    *
    * Note: This method doesn't guarantee any checkpointing semantics. It only updates the in-memory state of the last
    * processed offset and it is possible that the tasks during container restarts can start with offsets that are older
    * than the last processed offset.
    *
    * @param messageEnvelope incoming message envelope to be processed
    */
  def process(messageEnvelope: IncomingMessageEnvelope) = {
    val incomingMessageSSP = messageEnvelope.getSystemStreamPartition
    val storeName = sspsToStore.getOrElse(incomingMessageSSP,
      throw new SamzaException("Cannot locate the store associated with the %s" format incomingMessageSSP))

    debug("Processing %s for the side input store %s" format(incomingMessageSSP, storeName))

    val keyValueStore = taskStores.getOrElse(storeName,
      throw new SamzaException("Cannot locate the store associated with %s" format storeName)).asInstanceOf[KeyValueStore[Object, Object]]
    val entriesToBeWritten =
      sideInputProcessor.process(messageEnvelope, keyValueStore).asInstanceOf[util.Collection[Entry[Object, Object]]]
    keyValueStore.putAll(new util.ArrayList(entriesToBeWritten))

    debug("Successfully finished processing the message from %s for store %s" format(incomingMessageSSP, storeName))
    lastProcessedOffsetsForSideInputSSPs.put(incomingMessageSSP, messageEnvelope.getOffset)
  }

  /**
    * Fetch the starting offset of the given [[SystemStreamPartition]].
    *
    * Note: The method doesn't respect [[org.apache.samza.config.StreamConfig#CONSUMER_OFFSET_DEFAULT]] and
    * [[org.apache.samza.config.StreamConfig#CONSUMER_RESET_OFFSET]] configurations and will use the locally
    * checkpointed offset if its valid or fallback to oldest offset of the stream.
    *
    * @param ssp system stream partition for which the starting offset is requested
    *
    * @return the starting offset for the incoming [[SystemStreamPartition]]
    */
  def getStartingOffset(ssp: SystemStreamPartition) = startingOffsetForSideInputSSPs.get(ssp)

  /**
    * Fetch the last processed offset for the given [[SystemStreamPartition]].
    *
    * @param ssp system stream partition
    *
    * @return the last processed offset for the incoming [[SystemStreamPartition]]
    */
  def getLastProcessedOffset(ssp: SystemStreamPartition) = lastProcessedOffsetsForSideInputSSPs.get(ssp)
}
