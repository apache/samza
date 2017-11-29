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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.CheckpointListener;
import org.apache.samza.config.JobConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.kinesis.KinesisConfig;
import org.apache.samza.system.kinesis.metrics.KinesisSystemConsumerMetrics;
import org.apache.samza.util.BlockingEnvelopeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


/**
 * The system consumer for Kinesis, extending the {@link BlockingEnvelopeMap}.
 *
 * The system consumer creates a KinesisWorker per stream in it's own thread by providing a RecordProcessorFactory.
 * Kinesis Client Library (KCL) uses this factory to instantiate a KinesisRecordProcessor for each shard in the Kinesis
 * stream. KCL pushes data records to the appropriate record processor and the processor is responsible for processing
 * the resulting records and place them into a blocking queue in {@link BlockingEnvelopeMap}.
 *
 * <pre>
 *   {@code
 *                                                                                Shard1  +----------------------+
 *                                                                . --------------------> |KinesisRecordProcessor|
 *                        Stream1                                 |               Shard2  +----------------------+
 *                              +-------------+     +-----------------------------+       +----------------------+
 *             .--------------->|    Worker   |---->|    RecordProcessorFactory   | ----> |KinesisRecordProcessor|
 *             |                +-------------+     +-------------+---------------+       +----------------------+
 *             |                                                  |               Shard3  +----------------------+
 *             |                                                  . --------------------> |KinesisRecordProcessor|
 *             |                                                                          +----------------------+
 *             |          Stream2
 *  +---------------------+     +-------------+     +-----------------------------+        +-------+
 *  |KinesisSystemConsumer|---->|    Worker   |---->|    RecordProcessorFactory   |------->|  ...  |
 *  +---------------------+     +-------------+     +-----------------------------+        +-------+
 *             |
 *             |
 *             |
 *             |
 *             |                +-----------+
 *             . -------------->|    ...    |
 *                              +-----------+
 *  }
 *  </pre>
 * Since KinesisSystemConsumer uses KCL, the checkpoint state is stored in a dynamoDB table which is maintained by KCL.
 * KinesisSystemConsumer implements CheckpointListener to commit checkpoints via KCL.
 */

public class KinesisSystemConsumer extends BlockingEnvelopeMap implements CheckpointListener, KinesisRecordProcessorListener {

  private static final int MAX_BLOCKING_QUEUE_SIZE = 100;
  private static final Logger LOG = LoggerFactory.getLogger(KinesisSystemConsumer.class.getName());

  private final String system;
  private final KinesisConfig kConfig;
  private final KinesisSystemConsumerMetrics metrics;
  private final SSPAllocator sspAllocator;

  private final Set<String> streams = new HashSet<>();
  private final Map<SystemStreamPartition, KinesisRecordProcessor> processors = new ConcurrentHashMap<>();
  private final List<Worker> workers = new LinkedList<>();

  private ExecutorService executorService;

  private volatile Exception callbackException;

  public KinesisSystemConsumer(String systemName, KinesisConfig kConfig, MetricsRegistry registry) {
    super(registry, System::currentTimeMillis, null);
    this.system = systemName;
    this.kConfig = kConfig;
    this.metrics = new KinesisSystemConsumerMetrics(registry);
    this.sspAllocator = new SSPAllocator();
  }

  @Override
  protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue() {
    return new LinkedBlockingQueue<>(MAX_BLOCKING_QUEUE_SIZE);
  }

  @Override
  protected void put(SystemStreamPartition ssp, IncomingMessageEnvelope envelope) {
    try {
      super.put(ssp, envelope);
    } catch (Exception e) {
      LOG.error("Exception while putting record. Shutting down SystemStream {}", ssp.getSystemStream(), e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void register(SystemStreamPartition ssp, String offset) {
    LOG.info("Register called with ssp {} and offset {}. Offset will be ignored.", ssp, offset);
    String stream = ssp.getStream();
    streams.add(stream);
    sspAllocator.free(ssp);
    super.register(ssp, offset);
  }

  @Override
  public void start() {
    LOG.info("Start samza consumer for system {}.", system);

    metrics.initializeMetrics(streams);

    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat("kinesis-worker-thread-" + system + "-%d")
        .build();
    // launch kinesis workers in separate threads, one per stream
    executorService = Executors.newFixedThreadPool(streams.size(), namedThreadFactory);

    for (String stream : streams) {
      // KCL Dynamodb table is used for storing the state of processing. By default, the table name is the same as the
      // application name. Dynamodb table name must be unique for a given account and region (even across different
      // streams). So, let's create the default one with the combination of job name, job id and stream name. The table
      // name could be changed by providing a different TableName via KCL specific config.
      String kinesisApplicationName =
          kConfig.get(JobConfig.JOB_NAME()) + "-" + kConfig.get(JobConfig.JOB_ID()) + "-" + stream;

      Worker worker = new Worker.Builder()
          .recordProcessorFactory(createRecordProcessorFactory(stream))
          .config(kConfig.getKinesisClientLibConfig(system, stream, kinesisApplicationName))
          .build();

      workers.add(worker);

      // launch kinesis workers in separate thread-pools, one per stream
      executorService.execute(worker);
      LOG.info("Started worker for system {} stream {}.", system, stream);
    }
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> ssps, long timeout) throws InterruptedException {
    if (callbackException != null) {
      throw new SamzaException(callbackException);
    }
    return super.poll(ssps, timeout);
  }

  @Override
  public void stop() {
    LOG.info("Stop samza consumer for system {}.", system);
    workers.forEach(Worker::shutdown);
    workers.clear();
    executorService.shutdownNow();
    LOG.info("Kinesis system consumer executor service for system {} is shutdown.", system);
  }

  // package-private for tests
  IRecordProcessorFactory createRecordProcessorFactory(String stream) {
    return () -> {
      // This code is executed in Kinesis thread context.
      try {
        SystemStreamPartition ssp = sspAllocator.allocate(stream);
        KinesisRecordProcessor processor = new KinesisRecordProcessor(ssp, KinesisSystemConsumer.this);
        KinesisRecordProcessor prevProcessor = processors.put(ssp, processor);
        Validate.isTrue(prevProcessor == null, String.format("Adding new kinesis record processor %s while the"
                + " previous processor %s for the same ssp %s is still active.", processor, prevProcessor, ssp));
        return processor;
      } catch (Exception e) {
        callbackException = e;
        // This exception is the result of kinesis dynamic shard splits due to which sspAllocator ran out of free ssps.
        // Set the failed state in consumer which will eventually result in stopping the container. A manual job restart
        // will be required at this point. After the job restart, the newly created shards will be discovered and enough
        // ssps will be added to sspAllocator freePool.
        throw new SamzaException(e);
      }
    };
  }

  @Override
  public void onCheckpoint(Map<SystemStreamPartition, String> sspOffsets) {
    LOG.info("onCheckpoint called with sspOffsets {}", sspOffsets);
    sspOffsets.forEach((ssp, offset) -> {
        KinesisRecordProcessor processor = processors.get(ssp);
        KinesisSystemConsumerOffset kinesisOffset = KinesisSystemConsumerOffset.parse(offset);
        if (processor == null) {
          LOG.info("Kinesis Processor is not alive for ssp {}. This could be the result of rebalance. Hence dropping the"
              + " checkpoint {}.", ssp, offset);
        } else if (!kinesisOffset.getShardId().equals(processor.getShardId())) {
          LOG.info("KinesisProcessor for ssp {} currently owns shard {} while the checkpoint is for shard {}. This could"
              + " be the result of rebalance. Hence dropping the checkpoint {}.", ssp, processor.getShardId(),
              kinesisOffset.getShardId(), offset);
        } else {
          processor.checkpoint(kinesisOffset.getSeqNumber());
        }
      });
  }

  @Override
  public void onReceiveRecords(SystemStreamPartition ssp, List<Record> records, long millisBehindLatest) {
    metrics.updateMillisBehindLatest(ssp.getStream(), millisBehindLatest);
    records.forEach(record -> put(ssp, translate(ssp, record)));
  }

  @Override
  public void onShutdown(SystemStreamPartition ssp) {
    processors.remove(ssp);
    sspAllocator.free(ssp);
  }

  private IncomingMessageEnvelope translate(SystemStreamPartition ssp, Record record) {
    String shardId = processors.get(ssp).getShardId();
    byte[] payload = new byte[record.getData().remaining()];

    metrics.updateMetrics(ssp.getStream(), record);
    record.getData().get(payload);
    KinesisSystemConsumerOffset offset = new KinesisSystemConsumerOffset(shardId, record.getSequenceNumber());
    return new KinesisIncomingMessageEnvelope(ssp, offset.toString(), record.getPartitionKey(),
        payload, shardId, record.getSequenceNumber(), record.getApproximateArrivalTimestamp());
  }

}
