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

package org.apache.samza.container


import java.util.{Collections, Objects, Optional}
import java.util.concurrent.{CompletableFuture, ExecutorService, ScheduledExecutorService, Semaphore, TimeUnit}
import org.apache.samza.SamzaException
import org.apache.samza.checkpoint.kafka.{KafkaChangelogSSPOffset, KafkaStateCheckpointMarker}
import org.apache.samza.checkpoint.{CheckpointId, CheckpointV1, CheckpointV2, OffsetManager}
import org.apache.samza.config.{Config, StreamConfig, TaskConfig}
import org.apache.samza.context._
import org.apache.samza.job.model.{JobModel, TaskModel}
import org.apache.samza.scheduler.{CallbackSchedulerImpl, EpochTimeScheduler, ScheduledCallback}
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.storage.{ContainerStorageManager, TaskStorageCommitManager}
import org.apache.samza.system._
import org.apache.samza.table.TableManager
import org.apache.samza.task._
import org.apache.samza.util.ScalaJavaUtil.JavaOptionals.toRichOptional
import org.apache.samza.util.{Logging, ScalaJavaUtil}

import java.util
import java.util.concurrent.atomic.AtomicReference
import java.util.function.BiConsumer
import java.util.function.Function
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.{JavaConverters, Map}

class TaskInstance(
  val task: Any,
  taskModel: TaskModel,
  val metrics: TaskInstanceMetrics,
  systemAdmins: SystemAdmins,
  consumerMultiplexer: SystemConsumers,
  collector: TaskInstanceCollector,
  override val offsetManager: OffsetManager = new OffsetManager,
  commitManager: TaskStorageCommitManager = null,
  containerStorageManager: ContainerStorageManager = null,
  tableManager: TableManager = null,
  val systemStreamPartitions: java.util.Set[SystemStreamPartition] = Collections.emptySet(),
  val exceptionHandler: TaskInstanceExceptionHandler = new TaskInstanceExceptionHandler,
  jobModel: JobModel = null,
  streamMetadataCache: StreamMetadataCache = null,
  inputStreamMetadata: Map[SystemStream, SystemStreamMetadata] = Map(),
  timerExecutor: ScheduledExecutorService = null,
  commitThreadPool: ExecutorService = null,
  jobContext: JobContext,
  containerContext: ContainerContext,
  applicationContainerContextOption: Option[ApplicationContainerContext],
  applicationTaskContextFactoryOption: Option[ApplicationTaskContextFactory[ApplicationTaskContext]],
  externalContextOption: Option[ExternalContext]) extends Logging with RunLoopTask {

  val taskName: TaskName = taskModel.getTaskName
  val isInitableTask = task.isInstanceOf[InitableTask]
  val isEndOfStreamListenerTask = task.isInstanceOf[EndOfStreamListenerTask]
  val isClosableTask = task.isInstanceOf[ClosableTask]

  override val isWindowableTask = task.isInstanceOf[WindowableTask]

  override val epochTimeScheduler: EpochTimeScheduler = EpochTimeScheduler.create(timerExecutor)

  private val kvStoreSupplier = ScalaJavaUtil.toJavaFunction(
    (storeName: String) => {
      if (containerStorageManager != null) {
        val storeOption = containerStorageManager.getStore(taskName, storeName).toOption
        if (storeOption.isDefined) storeOption.get.asInstanceOf[KeyValueStore[_, _]] else null
      } else {
        null
      }
    })
  private val taskContext = new TaskContextImpl(taskModel, metrics.registry, kvStoreSupplier, tableManager,
    new CallbackSchedulerImpl(epochTimeScheduler), offsetManager, jobModel, streamMetadataCache)
  // need separate field for this instead of using it through Context, since Context throws an exception if it is null
  private val applicationTaskContextOption = applicationTaskContextFactoryOption
    .map(_.create(externalContextOption.orNull, jobContext, containerContext, taskContext,
      applicationContainerContextOption.orNull))
  val context = new ContextImpl(jobContext, containerContext, taskContext,
    Optional.ofNullable(applicationContainerContextOption.orNull),
    Optional.ofNullable(applicationTaskContextOption.orNull), Optional.ofNullable(externalContextOption.orNull))

  // store the (ssp -> if this ssp has caught up) mapping. "caught up"
  // means the same ssp in other taskInstances have the same offset as
  // the one here.
  var ssp2CaughtupMapping: scala.collection.mutable.Map[SystemStreamPartition, Boolean] =
    scala.collection.mutable.Map[SystemStreamPartition, Boolean]()
  systemStreamPartitions.foreach(ssp2CaughtupMapping += _ -> false)

  private val config: Config = jobContext.getConfig
  val taskConfig = new TaskConfig(config)

  val streamConfig: StreamConfig = new StreamConfig(config)
  override val intermediateStreams: java.util.Set[String] = JavaConverters.setAsJavaSetConverter(streamConfig.getStreamIds.filter(streamConfig.getIsIntermediateStream)).asJava

  val streamsToDeleteCommittedMessages: Set[String] = streamConfig.getStreamIds.filter(streamConfig.getDeleteCommittedMessages).map(streamConfig.getPhysicalName).toSet

  val checkpointWriteVersions = new TaskConfig(config).getCheckpointWriteVersions

  @volatile var lastCommitStartTimeMs = System.currentTimeMillis()
  @volatile var numSkippedCommits = 0
  val commitMaxDelayMs = taskConfig.getCommitMaxDelayMs
  val commitTimeoutMs = taskConfig.getCommitTimeoutMs
  val commitInProgress = new Semaphore(1)
  val commitException = new AtomicReference[Exception]()

  def registerOffsets {
    debug("Registering offsets for taskName: %s" format taskName)
    offsetManager.register(taskName, systemStreamPartitions)
  }

  def startTableManager {
    if (tableManager != null) {
      debug("Starting table manager for taskName: %s" format taskName)

      tableManager.init(context)
    } else {
      debug("Skipping table manager initialization for taskName: %s" format taskName)
    }
  }

  def initTask {
    initCaughtUpMapping()

    if (commitManager != null) {
      debug("Starting commit manager for taskName: %s" format taskName)

      commitManager.init()
    } else {
      debug("Skipping commit manager initialization for taskName: %s" format taskName)
    }

    if (offsetManager != null) {
      val checkpoint = offsetManager.getLastTaskCheckpoint(taskName)
      // Only required for checkpointV2
      if (checkpoint != null && checkpoint.getVersion == 2) {
        val checkpointV2 = checkpoint.asInstanceOf[CheckpointV2]
        // call cleanUp on backup managers in case the container previously failed during commit
        // before completing this step

        // WARNING: cleanUp is NOT optional with blob stores since this is where we reset the TTL for
        // tracked blobs. if this TTL reset is skipped, some of the blobs retained by future commits may
        // be deleted in the background by the blob store, leading to data loss.
        debug("Cleaning up stale state from previous run for taskName: %s" format taskName)
        commitManager.cleanUp(checkpointV2.getCheckpointId, checkpointV2.getStateCheckpointMarkers)
      }
    }

    if (taskConfig.getTransactionalStateRestoreEnabled() && taskConfig.getCommitMs > 0) {
      debug("Committing immediately on startup for taskName: %s so that the trimmed changelog " +
        "messages will be sealed in a checkpoint" format taskName)
      commit
    }

    if (isInitableTask) {
      debug("Initializing task for taskName: %s" format taskName)

      task.asInstanceOf[InitableTask].init(context)
    } else {
      debug("Skipping task initialization for taskName: %s" format taskName)
    }
    applicationTaskContextOption.foreach(applicationTaskContext => {
      debug("Starting application-defined task context for taskName: %s" format taskName)
      applicationTaskContext.start()
    })
  }

  def registerProducers {
    debug("Registering producers for taskName: %s" format taskName)

    collector.register
  }

  /**
    * Computes the starting offset for the partitions assigned to the task and registers them with the underlying {@see SystemConsumers}.
    *
    * Starting offset for a partition of the task is computed in the following manner:
    *
    * 1. If a startpoint exists for a task, system stream partition and it resolves to a offset, then the resolved offset is used as the starting offset.
    * 2. Else, the checkpointed offset for the system stream partition is used as the starting offset.
    */
  def registerConsumers() {
    debug("Registering consumers for taskName: %s" format taskName)
    systemStreamPartitions.foreach(systemStreamPartition => {
      val startingOffset: String = getStartingOffset(systemStreamPartition)
      consumerMultiplexer.register(systemStreamPartition, startingOffset)
      metrics.addOffsetGauge(systemStreamPartition, () => offsetManager.getLastProcessedOffset(taskName, systemStreamPartition).orNull)
    })
  }

  def process(envelope: IncomingMessageEnvelope, coordinator: ReadableCoordinator,
    callbackFactory: TaskCallbackFactory) {
    metrics.processes.inc

    val incomingMessageSsp = envelope.getSystemStreamPartition

    if (!ssp2CaughtupMapping.getOrElse(incomingMessageSsp,
      throw new SamzaException(incomingMessageSsp + " is not registered!"))) {
      checkCaughtUp(envelope)
    }

    if (ssp2CaughtupMapping(incomingMessageSsp)) {
      metrics.messagesActuallyProcessed.inc

      // TODO BLOCKER pmaheshw reenable after demo
//      trace("Processing incoming message envelope for taskName: %s SSP: %s offset: %s"
//        format (taskName, incomingMessageSsp, envelope.getOffset))

      exceptionHandler.maybeHandle {
        val callback = callbackFactory.createCallback()
        task.asInstanceOf[AsyncStreamTask].processAsync(envelope, collector, coordinator, callback)
      }
    }
  }

  def endOfStream(coordinator: ReadableCoordinator): Unit = {
    if (isEndOfStreamListenerTask) {
      exceptionHandler.maybeHandle {
        task.asInstanceOf[EndOfStreamListenerTask].onEndOfStream(collector, coordinator)
      }
    }
  }

  def window(coordinator: ReadableCoordinator) {
    if (isWindowableTask) {
      trace("Windowing for taskName: %s" format taskName)

      metrics.windows.inc

      exceptionHandler.maybeHandle {
        task.asInstanceOf[WindowableTask].window(collector, coordinator)
      }
    }
  }

  def scheduler(coordinator: ReadableCoordinator) {
    trace("Scheduler for taskName: %s" format taskName)

    exceptionHandler.maybeHandle {
      epochTimeScheduler.removeReadyTimers().entrySet().foreach { entry =>
        entry.getValue.asInstanceOf[ScheduledCallback[Any]].onCallback(entry.getKey.getKey, collector, coordinator)
      }
    }
  }

  def commit {
    // TODO BLOCKER pmaheshw add tests.
    // ensure that only one commit (including sync and async phases) is ever in progress for a task.

    val commitStartNs = System.nanoTime()
    // first check if there were any unrecoverable errors during the async stage of the pending commit
    // and if so, shut down the container.
    if (commitException.get() != null) {
      throw new SamzaException("Unrecoverable error during pending commit for taskName: %s." format taskName,
        commitException.get())
    }

    // if no commit is in progress for this task, continue with this commit.
    // if a previous commit is in progress but less than {@code task.commit.max.delay.ms}
    // have elapsed since it started, skip this commit request.
    // if more time has elapsed than that, block this commit until the previous commit
    // is complete, then continue with this commit.
    if (!commitInProgress.tryAcquire()) {
      val timeSinceLastCommit = System.currentTimeMillis() - lastCommitStartTimeMs
      if (timeSinceLastCommit < commitMaxDelayMs) {
        info("Skipping commit for taskName: %s since another commit is in progress. " +
          "%s ms have elapsed since the pending commit started." format (taskName, timeSinceLastCommit))
        metrics.asyncCommitSkipped.set(numSkippedCommits + 1)
        return
      } else {
        warn("Blocking processing for taskName: %s until in-flight commit is complete. " +
          "%s ms have elapsed since the pending commit started, " +
          "which is greater than the max allowed commit delay: %s."
          format (taskName, timeSinceLastCommit, commitMaxDelayMs))

        if (!commitInProgress.tryAcquire(commitTimeoutMs, TimeUnit.MILLISECONDS)) {
          val timeSinceLastCommit = System.currentTimeMillis() - lastCommitStartTimeMs
          throw new SamzaException("Timeout waiting for pending commit for taskName: %s to finish. " +
            "%s ms have elapsed since the pending commit started. Max allowed commit delay is %s ms " +
            "and commit timeout beyond that is %s ms" format (taskName, timeSinceLastCommit,
            commitMaxDelayMs, commitTimeoutMs))
        }
      }
    }
    // at this point the permit for semaphore has been acquired, proceed with commit.
    // the first part of the commit needs to be exclusive with processing, so do it on the caller thread.
    lastCommitStartTimeMs = System.currentTimeMillis()

    metrics.commits.inc
    val checkpointId = CheckpointId.create()

    debug("Starting sync stage of commit for taskName: %s checkpointId: %s" format (taskName, checkpointId))

    val inputOffsets = offsetManager.getLastProcessedOffsets(taskName)
    trace("Got last processed input offsets for taskName: %s checkpointId: %s as: %s"
      format(taskName, checkpointId, inputOffsets))

    trace("Flushing producers for taskName: %s checkpointId: %s" format (taskName, checkpointId))
    // Flushes output, checkpoint and changelog producers
    collector.flush

    if (tableManager != null) {
      trace("Flushing tables for taskName: %s checkpointId: %s" format (taskName, checkpointId))
      tableManager.flush()
    }

    // create a synchronous snapshot of stores for commit
    debug("Creating synchronous state store snapshots for taskName: %s checkpointId: %s"
      format (taskName, checkpointId))
    val snapshotStartTimeNs = System.nanoTime()
    val snapshotSCMs = commitManager.snapshot(checkpointId)

    metrics.snapshotNs.update(System.nanoTime() - snapshotStartTimeNs)
    trace("Got synchronous snapshot SCMs for taskName: %s checkpointId: %s as: %s "
      format(taskName, checkpointId, snapshotSCMs))

    debug("Submitting async stage of commit for taskName: %s checkpointId: %s for execution"
      format (taskName, checkpointId))
    // rest of the commit can happen asynchronously and concurrently with processing.
    // schedule it on the commit executor and return. submitted runnable releases the
    // commit semaphore permit when this commit is complete.
    commitThreadPool.submit(new Runnable {
      override def run(): Unit = {
        debug("Starting async stage of commit for taskName: %s checkpointId: %s" format (taskName, checkpointId))

        try {
          val uploadStartTimeNs = System.nanoTime()
          val uploadSCMsFuture = commitManager.upload(checkpointId, snapshotSCMs)

          uploadSCMsFuture.whenComplete(new BiConsumer[util.Map[String, util.Map[String, String]], Throwable] {
            override def accept(t: util.Map[String, util.Map[String, String]], throwable: Throwable): Unit = {
              if (throwable == null) {
                metrics.asyncUploadNs.update(System.nanoTime() - uploadStartTimeNs)
                metrics.asyncUploadsCompleted.inc()
              } else {
                debug("Commit upload did not complete successfully for taskName: %s checkpointId: %s with error msg: %s"
                  format (taskName, checkpointId, throwable.getMessage))
              }
            }
          })

          // explicit types required to make scala compiler happy
          val checkpointWriteFuture: CompletableFuture[util.Map[String, util.Map[String, String]]] =
            uploadSCMsFuture.thenApplyAsync(writeCheckpoint(checkpointId, inputOffsets), commitThreadPool)

          val cleanUpFuture: CompletableFuture[Void] =
            checkpointWriteFuture.thenComposeAsync(cleanUp(checkpointId), commitThreadPool)

          val trimFuture = cleanUpFuture.thenRunAsync(
            trim(checkpointId, inputOffsets), commitThreadPool)

          trimFuture.whenCompleteAsync(handleCompletion(checkpointId, commitStartNs), commitThreadPool)
        } catch {
          case t: Throwable => handleCompletion(checkpointId, commitStartNs).accept(null, t)
        }
      }
    })

    debug("Finishing sync stage of commit for taskName: %s checkpointId: %s" format (taskName, checkpointId))
  }

  private def writeCheckpoint(checkpointId: CheckpointId, inputOffsets: util.Map[SystemStreamPartition, String]) = {
    new Function[util.Map[String, util.Map[String, String]], util.Map[String, util.Map[String, String]]]() {
      override def apply(uploadSCMs: util.Map[String, util.Map[String, String]]) = {
        trace("Got asynchronous upload SCMs for taskName: %s checkpointId: %s as: %s "
          format(taskName, checkpointId, uploadSCMs))

        debug("Creating and writing checkpoints for taskName: %s checkpointId: %s" format (taskName, checkpointId))
        checkpointWriteVersions.foreach(checkpointWriteVersion => {
          val checkpoint = if (checkpointWriteVersion == 1) {
            // build CheckpointV1 with KafkaChangelogSSPOffset for backwards compatibility
            val allCheckpointOffsets = new util.HashMap[SystemStreamPartition, String]()
            allCheckpointOffsets.putAll(inputOffsets)
            val newestChangelogOffsets = KafkaStateCheckpointMarker.scmsToSSPOffsetMap(uploadSCMs)
            newestChangelogOffsets.foreach { case (ssp, newestOffsetOption) =>
              val offset = new KafkaChangelogSSPOffset(checkpointId, newestOffsetOption.orNull).toString
              allCheckpointOffsets.put(ssp, offset)
            }
            new CheckpointV1(allCheckpointOffsets)
          } else if (checkpointWriteVersion == 2) {
            new CheckpointV2(checkpointId, inputOffsets, uploadSCMs)
          } else {
            throw new SamzaException("Unsupported checkpoint write version: " + checkpointWriteVersion)
          }

          trace("Writing checkpoint for taskName: %s checkpointId: %s as: %s"
            format(taskName, checkpointId, checkpoint))

          // Write input offsets and state checkpoint markers to task store and checkpoint directories
          commitManager.writeCheckpointToStoreDirectories(checkpoint)

          // Write input offsets and state checkpoint markers to the checkpoint topic atomically
          offsetManager.writeCheckpoint(taskName, checkpoint)
        })

        uploadSCMs
      }
    }
  }

  private def cleanUp(checkpointId: CheckpointId) = {
    new Function[util.Map[String, util.Map[String, String]], CompletableFuture[Void]] {
      override def apply(uploadSCMs: util.Map[String, util.Map[String, String]]): CompletableFuture[Void] = {
        // Perform cleanup on unused checkpoints
        debug("Cleaning up old checkpoint state for taskName: %s checkpointId: %s" format(taskName, checkpointId))
        try {
          commitManager.cleanUp(checkpointId, uploadSCMs)
        } catch {
          case e: Exception =>
            // WARNING: cleanUp is NOT optional with blob stores since this is where we reset the TTL for
            // tracked blobs. if this TTL reset is skipped, some of the blobs retained by future commits may
            // be deleted in the background by the blob store, leading to data loss.
            throw new SamzaException(
              "Failed to remove old checkpoint state for taskName: %s checkpointId: %s."
                format(taskName, checkpointId), e)
        }
      }
    }
  }

  private def trim(checkpointId: CheckpointId, inputOffsets: util.Map[SystemStreamPartition, String]) = {
    new Runnable {
      override def run(): Unit = {
        trace("Deleting committed input offsets from intermediate topics for taskName: %s checkpointId: %s"
          format (taskName, checkpointId))
        inputOffsets.asScala
          .filter { case (ssp, _) => streamsToDeleteCommittedMessages.contains(ssp.getStream) } // Only delete data of intermediate streams
          .groupBy { case (ssp, _) => ssp.getSystem }
          .foreach { case (systemName: String, offsets: Map[SystemStreamPartition, String]) =>
            systemAdmins.getSystemAdmin(systemName).deleteMessages(offsets.asJava)
          }
      }
    }
  }

  private def handleCompletion(checkpointId: CheckpointId, commitStartNs: Long) = {
    new BiConsumer[Void, Throwable] {
      override def accept(v: Void, e: Throwable): Unit = {
        try {
          debug("%s finishing async stage of commit for taskName: %s checkpointId: %s."
            format (if (e == null) "Successfully" else "Unsuccessfully", taskName, checkpointId))
          if (e != null) {
            val exception = new SamzaException("Unrecoverable error during async stage of commit " +
              "for taskName: %s checkpointId: %s" format(taskName, checkpointId), e)
            val exceptionSet = commitException.compareAndSet(null, exception)
            if (!exceptionSet) {
              // should never happen because there should be at most one async stage of commit in progress
              // for a task and another one shouldn't be schedule if the previous one failed. throw a new
              // exception on the caller thread for logging and debugging if this happens.
              error("Should not have encountered a non-null saved exception during async stage of " +
                "commit for taskName: %s checkpointId: %s" format(taskName, checkpointId), commitException.get())
              error("New exception during async stage of commit for taskName: %s checkpointId: %s"
                format(taskName, checkpointId), exception)
              throw new SamzaException("Should not have encountered a non-null saved exception " +
                "during async stage of commit for taskName: %s checkpointId: %s. New exception logged above. " +
                "Saved exception under Caused By.", commitException.get())
            }
          } else {
            metrics.commitNs.update(System.nanoTime() - commitStartNs)
            // reset the numbers skipped commits for the current commit
            numSkippedCommits = 0
            metrics.asyncCommitSkipped.set(numSkippedCommits)
          }
        } finally {
          // release the permit indicating that previous commit is complete.
          commitInProgress.release()
        }
      }
    }
  }

  def shutdownTask {
    if (commitManager != null) {
      debug("Shutting down commit manager for taskName: %s" format taskName)
      commitManager.close()
    } else {
      debug("Skipping commit manager shutdown for taskName: %s" format taskName)
    }
    applicationTaskContextOption.foreach(applicationTaskContext => {
      debug("Stopping application-defined task context for taskName: %s" format taskName)
      applicationTaskContext.stop()
    })
    if (task.isInstanceOf[ClosableTask]) {
      debug("Shutting down stream task for taskName: %s" format taskName)

      task.asInstanceOf[ClosableTask].close
    } else {
      debug("Skipping stream task shutdown for taskName: %s" format taskName)
    }
  }

  def shutdownTableManager {
    if (tableManager != null) {
      debug("Shutting down table manager for taskName: %s" format taskName)

      tableManager.close
    } else {
      debug("Skipping table manager shutdown for taskName: %s" format taskName)
    }
  }

  override def toString() = "TaskInstance for class %s and taskName %s." format (task.getClass.getName, taskName)

  def toDetailedString() = "TaskInstance [taskName = %s, windowable=%s, closable=%s endofstreamlistener=%s]" format
    (taskName, isWindowableTask, isClosableTask, isEndOfStreamListenerTask)

  /**
   * From the envelope, check if this SSP has caught up with the starting offset of the SSP
   * in this TaskInstance. If the offsets are not comparable, default to true, which means
   * it's already caught up.
   */
  private def checkCaughtUp(envelope: IncomingMessageEnvelope) = {
    val incomingMessageSsp = envelope.getSystemStreamPartition

    if (IncomingMessageEnvelope.END_OF_STREAM_OFFSET.equals(envelope.getOffset)) {
      ssp2CaughtupMapping(incomingMessageSsp) = true
    } else {
      systemAdmins match {
        case null => {
          warn("systemAdmin is null. Set all SystemStreamPartitions to caught-up")
          ssp2CaughtupMapping(incomingMessageSsp) = true
        }
        case others => {
          val startingOffset = getStartingOffset(incomingMessageSsp)

          val system = incomingMessageSsp.getSystem
          others.getSystemAdmin(system).offsetComparator(envelope.getOffset, startingOffset) match {
            case null => {
              info("offsets in " + system + " is not comparable. Set all SystemStreamPartitions to caught-up")
              ssp2CaughtupMapping(incomingMessageSsp) = true // not comparable
            }
            case result => {
              if (result >= 0) {
                info(incomingMessageSsp.toString + " has caught up.")
                ssp2CaughtupMapping(incomingMessageSsp) = true
              }
            }
          }
        }
      }
    }
  }

  /**
    * Check each partition assigned to the task is caught to the last offset
    */
  def initCaughtUpMapping() {
    if (inputStreamMetadata != null && inputStreamMetadata.nonEmpty) {
      systemStreamPartitions.foreach(ssp => {
        if (inputStreamMetadata.contains(ssp.getSystemStream)) {
          val partitionMetadata = inputStreamMetadata(ssp.getSystemStream)
            .getSystemStreamPartitionMetadata.get(ssp.getPartition)

          val upcomingOffset = partitionMetadata.getUpcomingOffset
          val startingOffset = offsetManager.getStartingOffset(taskName, ssp)
            .getOrElse(throw new SamzaException("No offset defined for SystemStreamPartition: %s" format ssp))

          // Mark ssp to be caught up if the starting offset is already the
          // upcoming offset, meaning the task has consumed all the messages
          // in this partition before and waiting for the future incoming messages.
          if(Objects.equals(upcomingOffset, startingOffset)) {
            ssp2CaughtupMapping(ssp) = true
          }
        }
      })
    }
  }

  private def getStartingOffset(systemStreamPartition: SystemStreamPartition) = {
    val offset = offsetManager.getStartingOffset(taskName, systemStreamPartition)
    val startingOffset = offset.getOrElse(
      throw new SamzaException("No offset defined for SystemStreamPartition: %s" format systemStreamPartition))

    startingOffset
  }
}
