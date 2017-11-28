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

package org.apache.samza.system.kinesis.consumer;

import java.util.List;

import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;


/**
 * Record processor for AWS kinesis stream. It does the following:
 * <ul>
 *   <li> when a shard is assigned by KCL in initialize API, it asks and gets an ssp from sspAllocator.
 *   <li> when records are received in processRecords API, it translates them to IncomingMessageEnvelope and enqueues
 *        the resulting envelope in the appropriate blocking buffer queue.
 *   <li> when checkpoint API is called by samza, it checkpoints via KCL to Kinesis.
 *   <li> when shutdown API is called by KCL, based on the terminate reason, it takes necessary action.
 * </ul>
 *
 * initialize, processRecords and shutdown APIs are never called concurrently on a processor instance. However,
 * checkpoint API could be called by Samza thread while processRecords and shutdown callback APIs are invoked by KCL.
 * Please note that the APIs for different record processor instances could be called concurrently.
 */

public class KinesisRecordProcessor implements IRecordProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(KinesisRecordProcessor.class.getName());
  static final long POLL_INTERVAL_DURING_PARENT_SHARD_SHUTDOWN_MS = 1000;

  private final SystemStreamPartition ssp;

  private String shardId;
  private KinesisRecordProcessorListener listener;
  private IRecordProcessorCheckpointer checkpointer;
  private ExtendedSequenceNumber initSeqNumber;

  private volatile ExtendedSequenceNumber lastProcessedRecordSeqNumber;
  private volatile ExtendedSequenceNumber lastCheckpointedRecordSeqNumber;

  private boolean shutdownRequested = false;

  KinesisRecordProcessor(SystemStreamPartition ssp, KinesisRecordProcessorListener listener) {
    this.ssp = ssp;
    this.listener = listener;
  }

  /**
   * Invoked by the Amazon Kinesis Client Library before data records are delivered to the RecordProcessor instance
   * (via processRecords).
   *
   * @param initializationInput Provides information related to initialization
   */
  @Override
  public void initialize(InitializationInput initializationInput) {
    Validate.isTrue(listener != null, "There is no listener set for the processor.");
    initSeqNumber = initializationInput.getExtendedSequenceNumber();
    shardId = initializationInput.getShardId();
    LOG.info("Initialization done for {} with sequence {}", this,
        initializationInput.getExtendedSequenceNumber().getSequenceNumber());
  }

  /**
   * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
   * application. Upon fail over, the new instance will get records with sequence number greater than the checkpoint
   * position for each partition key.
   *
   * @param processRecordsInput Provides the records to be processed as well as information and capabilities related
   *        to them (eg checkpointing).
   */
  @Override
  public void processRecords(ProcessRecordsInput processRecordsInput) {
    // KCL does not send any records to the processor that was shutdown.
    Validate.isTrue(!shutdownRequested,
        String.format("KCL returned records after shutdown is called on the processor %s.", this));
    // KCL aways gives reference to the same checkpointer instance for a given processor instance.
    checkpointer = processRecordsInput.getCheckpointer();
    List<Record> records = processRecordsInput.getRecords();
    // Empty records are expected when KCL config has CallProcessRecordsEvenForEmptyRecordList set to true.
    if (!records.isEmpty()) {
      lastProcessedRecordSeqNumber = new ExtendedSequenceNumber(records.get(records.size() - 1).getSequenceNumber());
      listener.onReceiveRecords(ssp, records, processRecordsInput.getMillisBehindLatest());
    }
  }

  /**
   * Invoked by the Samza thread to commit checkpoint for the shard owned by the record processor instance.
   *
   * @param seqNumber sequenceNumber to checkpoint for the shard owned by this processor instance.
   */
  public void checkpoint(String seqNumber) {
    ExtendedSequenceNumber seqNumberToCheckpoint = new ExtendedSequenceNumber(seqNumber);
    if (initSeqNumber.compareTo(seqNumberToCheckpoint) > 0) {
      LOG.warn("Samza called checkpoint with seqNumber {} smaller than initial seqNumber {} for {}. Ignoring it!",
          seqNumber, initSeqNumber, this);
      return;
    }

    if (checkpointer == null) {
      // checkpointer could be null as a result of shard re-assignment before the first record is processed.
      LOG.warn("Ignoring checkpointing for {} with seqNumber {} because of re-assignment.", this, seqNumber);
      return;
    }

    try {
      checkpointer.checkpoint(seqNumber);
      lastCheckpointedRecordSeqNumber = seqNumberToCheckpoint;
    } catch (ShutdownException e) {
      // This can happen as a result of shard re-assignment.
      String msg = String.format("Checkpointing %s with seqNumber %s failed with exception. Dropping the checkpoint.",
          this, seqNumber);
      LOG.warn(msg, e);
    } catch (InvalidStateException e) {
      // This can happen when KCL encounters issues with internal state, eg: dynamoDB table is not found
      String msg =
          String.format("Checkpointing %s with seqNumber %s failed with exception.", this, seqNumber);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    } catch (ThrottlingException e) {
      // Throttling is handled by KCL via the client lib configuration properties. If we get an exception inspite of
      // throttling back-off behavior, let's throw an exception as the configs
      String msg = String.format("Checkpointing %s with seqNumber %s failed with exception. Checkpoint interval is"
              + " too aggressive for the provisioned throughput of the dynamoDB table where the checkpoints are stored."
              + " Either reduce the checkpoint interval -or- increase the throughput of dynamoDB table.", this,
          seqNumber);
      throw new SamzaException(msg);
    }
  }

  /**
   * Invoked by the Amazon Kinesis Client Library to indicate it will no longer send data records to this
   * RecordProcessor instance.
   *
   * @param shutdownInput Provides information and capabilities (eg checkpointing) related to shutdown of this record
   *        processor.
   */
  @Override
  public void shutdown(ShutdownInput shutdownInput) {
    LOG.info("Shutting down {} with reason:{}", this, shutdownInput.getShutdownReason());

    Validate.isTrue(!shutdownRequested, String.format("KCL called shutdown more than once for processor %s.", this));
    shutdownRequested = true;
    // shutdown reason TERMINATE indicates that the shard is closed due to re-sharding. It also indicates that all the
    // records from the shard have been delivered to the consumer and the consumer is expected to checkpoint the
    // progress.
    if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
      // We need to ensure that all records are processed and checkpointed before going ahead and marking the processing
      // complete by calling checkpoint() on KCL. We need to checkpoint the completion state for parent shard, for KCL
      // to consume from the child shard(s).
      try {
        LOG.info("Waiting for all the records for {} to be processed.", this);
        // Let's poll periodically and block until the last processed record is checkpointed. Also handle the case
        // where there are no records received for the processor, in which case the lastProcessedRecordSeqNumber will
        // be null.
        while (lastProcessedRecordSeqNumber != null
            && !lastProcessedRecordSeqNumber.equals(lastCheckpointedRecordSeqNumber)) {
          Thread.sleep(POLL_INTERVAL_DURING_PARENT_SHARD_SHUTDOWN_MS);
        }
        LOG.info("Final checkpoint for {} before shutting down.", this);
        shutdownInput.getCheckpointer().checkpoint();
      } catch (Exception e) {
        LOG.warn("An error occurred while committing the final checkpoint in the parent shard {}", this, e);
      }
    }
    listener.onShutdown(ssp);
  }

  String getShardId() {
    return shardId;
  }

  @Override
  public String toString() {
    return String.format("KinesisRecordProcessor: ssp %s shard %s hashCode %s", ssp, shardId, hashCode());
  }
}
