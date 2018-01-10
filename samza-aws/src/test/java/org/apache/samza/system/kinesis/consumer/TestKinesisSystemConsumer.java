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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.model.Record;

import org.apache.samza.system.kinesis.KinesisConfig;
import org.apache.samza.system.kinesis.metrics.KinesisSystemConsumerMetrics;

import static org.apache.samza.system.kinesis.consumer.TestKinesisRecordProcessor.*;
import static org.mockito.Mockito.*;


/**
 * These class of tests test KinesisSystemConsumer and KinesisRecordProcessor together.
 */
public class TestKinesisSystemConsumer {
  private static final String SYSTEM_CONSUMER_REGISTER_OFFSET = "0000"; // Could be any string

  @Test
  public void testProcessRecords() throws InterruptedException, ShutdownException, InvalidStateException,
                                          NoSuchFieldException, IllegalAccessException {
    String system = "kinesis";
    String stream = "stream";
    int numShards = 2;
    int numRecordsPerShard = 5;

    testProcessRecordsHelper(system, stream, numShards, numRecordsPerShard);
  }

  @Test
  public void testProcessRecordsWithEmptyRecordList() throws InterruptedException, ShutdownException,
                                                             InvalidStateException, NoSuchFieldException,
                                                             IllegalAccessException {
    String system = "kinesis";
    String stream = "stream";
    int numShards = 1;
    int numRecordsPerShard = 0;

    testProcessRecordsHelper(system, stream, numShards, numRecordsPerShard);
  }

  /**
   * Helper to simulate and test the life-cycle of record processing from a kinesis stream with a given number of shards
   * 1. Creation of record processors.
   * 2. Initialization of record processors.
   * 3. Processing records via record processors.
   * 4. Calling checkpoint on record processors.
   * 5. Shutting down (due to re-assignment or lease expiration) record processors.
   */
  private void testProcessRecordsHelper(String system, String stream, int numShards, int numRecordsPerShard)
      throws InterruptedException, ShutdownException, InvalidStateException,
             NoSuchFieldException, IllegalAccessException {

    KinesisConfig kConfig = new KinesisConfig(new MapConfig());
    // Create consumer
    KinesisSystemConsumer consumer = new KinesisSystemConsumer(system, kConfig, new NoOpMetricsRegistry());
    initializeMetrics(consumer, stream);

    List<SystemStreamPartition> ssps = new LinkedList<>();
    IntStream.range(0, numShards)
        .forEach(p -> {
            SystemStreamPartition ssp = new SystemStreamPartition(system, stream, new Partition(p));
            ssps.add(ssp);
          });
    ssps.forEach(ssp -> consumer.register(ssp, SYSTEM_CONSUMER_REGISTER_OFFSET));

    // Create Kinesis record processor factory
    IRecordProcessorFactory factory = consumer.createRecordProcessorFactory(stream);

    // Create and initialize Kinesis record processor
    Map<String, KinesisRecordProcessor> processorMap = createAndInitProcessors(factory, numShards);
    List<KinesisRecordProcessor> processorList = new ArrayList<>(processorMap.values());

    // Generate records to Kinesis record processor
    Map<KinesisRecordProcessor, List<Record>> inputRecordMap = generateRecords(numRecordsPerShard, processorList);

    // Verification steps

    // Read events from the BEM queue
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> messages =
        readEvents(new HashSet<>(ssps), consumer, numRecordsPerShard);
    if (numRecordsPerShard > 0) {
      Assert.assertEquals(messages.size(), numShards);
    } else {
      // No input records and hence no messages
      Assert.assertEquals(messages.size(), 0);
      return;
    }

    Map<SystemStreamPartition, KinesisRecordProcessor> sspToProcessorMap = getProcessorMap(consumer);
    ssps.forEach(ssp -> {
        try {
          KinesisRecordProcessor processor = sspToProcessorMap.get(ssp);

          if (numRecordsPerShard > 0) {
            // Verify that the read messages are received in order and are the same as input records
            Assert.assertEquals(messages.get(ssp).size(), numRecordsPerShard);
            List<IncomingMessageEnvelope> envelopes = messages.get(ssp);
            List<Record> inputRecords = inputRecordMap.get(processor);
            verifyRecords(envelopes, inputRecords, processor.getShardId());

            // Call checkpoint on consumer and verify that the checkpoint is called with the right offset
            IncomingMessageEnvelope lastEnvelope = envelopes.get(envelopes.size() - 1);
            consumer.onCheckpoint(Collections.singletonMap(ssp, lastEnvelope.getOffset()));
            ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
            verify(getCheckpointer(processor)).checkpoint(argument.capture());
            Assert.assertEquals(inputRecords.get(inputRecords.size() - 1).getSequenceNumber(), argument.getValue());
          }

          // Call shutdown (with ZOMBIE reason) on processor and verify if shutdown freed the ssp mapping
          shutDownProcessor(processor, ShutdownReason.ZOMBIE);
          Assert.assertTrue(!sspToProcessorMap.containsValue(processor));
          Assert.assertTrue(isSspAvailable(consumer, ssp));
        } catch (NoSuchFieldException | IllegalAccessException | InvalidStateException | ShutdownException ex) {
          throw new RuntimeException(ex);
        }
      });
  }

  private Map<String, KinesisRecordProcessor> createAndInitProcessors(IRecordProcessorFactory factory, int numShards) {
    Map<String, KinesisRecordProcessor> processorMap = new HashMap<>();
    IntStream.range(0, numShards)
        .forEach(p -> {
            String shardId = String.format("shard-%05d", p);
            // Create Kinesis processor
            KinesisRecordProcessor processor = (KinesisRecordProcessor) factory.createProcessor();

            // Initialize the shard
            ExtendedSequenceNumber seqNum = new ExtendedSequenceNumber("0000");
            InitializationInput initializationInput =
                new InitializationInput().withShardId(shardId).withExtendedSequenceNumber(seqNum);
            processor.initialize(initializationInput);
            processorMap.put(shardId, processor);
          });
    return processorMap;
  }

  private Map<SystemStreamPartition, List<IncomingMessageEnvelope>> readEvents(Set<SystemStreamPartition> ssps,
      KinesisSystemConsumer consumer, int numEvents) throws InterruptedException {
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> messages = new HashMap<>();
    int totalEventsConsumed = 0;

    while (totalEventsConsumed < numEvents) {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> receivedMessages =
          consumer.poll(ssps, Duration.ofSeconds(1).toMillis());
      receivedMessages.forEach((key, value) -> {
          if (messages.containsKey(key)) {
            messages.get(key).addAll(value);
          } else {
            messages.put(key, new ArrayList<>(value));
          }
        });
      totalEventsConsumed = messages.values().stream().mapToInt(List::size).sum();
    }

    if (totalEventsConsumed < numEvents) {
      String msg = String.format("Received only %d of %d events", totalEventsConsumed, numEvents);
      throw new SamzaException(msg);
    }
    return messages;
  }

  private void verifyRecords(List<IncomingMessageEnvelope> outputRecords, List<Record> inputRecords, String shardId) {
    Iterator outputRecordsIter = outputRecords.iterator();
    inputRecords.forEach(record -> {
        IncomingMessageEnvelope envelope = (IncomingMessageEnvelope) outputRecordsIter.next();
        String outputKey = (String) envelope.getKey();
        KinesisIncomingMessageEnvelope kinesisMessageEnvelope = (KinesisIncomingMessageEnvelope) envelope;
        Assert.assertEquals(outputKey, record.getPartitionKey());
        Assert.assertEquals(kinesisMessageEnvelope.getSequenceNumber(), record.getSequenceNumber());
        Assert.assertEquals(kinesisMessageEnvelope.getApproximateArrivalTimestamp(),
            record.getApproximateArrivalTimestamp());
        Assert.assertEquals(kinesisMessageEnvelope.getShardId(), shardId);
        ByteBuffer outputData = ByteBuffer.wrap((byte[]) kinesisMessageEnvelope.getMessage());
        record.getData().rewind();
        Assert.assertTrue(outputData.equals(record.getData()));
        verifyOffset(envelope.getOffset(), record, shardId);
      });
  }

  private void verifyOffset(String offset, Record inputRecord, String shardId) {
    KinesisSystemConsumerOffset ckpt = KinesisSystemConsumerOffset.parse(offset);
    Assert.assertEquals(ckpt.getSeqNumber(), inputRecord.getSequenceNumber());
    Assert.assertEquals(ckpt.getShardId(), shardId);
  }

  @SuppressWarnings("unchecked")
  private void initializeMetrics(KinesisSystemConsumer consumer, String stream)
      throws NoSuchFieldException, IllegalAccessException {
    Field f = consumer.getClass().getDeclaredField("metrics");
    f.setAccessible(true);
    KinesisSystemConsumerMetrics metrics = (KinesisSystemConsumerMetrics) f.get(consumer);
    metrics.initializeMetrics(Collections.singleton(stream));
  }

  @SuppressWarnings("unchecked")
  private Map<SystemStreamPartition, KinesisRecordProcessor> getProcessorMap(KinesisSystemConsumer consumer)
      throws NoSuchFieldException, IllegalAccessException {
    Field f = consumer.getClass().getDeclaredField("processors");
    f.setAccessible(true);
    return (Map<SystemStreamPartition, KinesisRecordProcessor>) f.get(consumer);
  }

  @SuppressWarnings("unchecked")
  private boolean isSspAvailable(KinesisSystemConsumer consumer, SystemStreamPartition ssp)
      throws NoSuchFieldException, IllegalAccessException {
    SSPAllocator sspAllocator = getSspAllocator(consumer);
    Field f = sspAllocator.getClass().getDeclaredField("availableSsps");
    f.setAccessible(true);
    Map<String, Set<SystemStreamPartition>> availableSsps = (Map<String, Set<SystemStreamPartition>>) f.get(
        sspAllocator);
    return availableSsps.containsKey(ssp.getStream()) && availableSsps.get(ssp.getStream()).contains(ssp);
  }

  private SSPAllocator getSspAllocator(KinesisSystemConsumer consumer)
      throws NoSuchFieldException, IllegalAccessException {
    Field f = consumer.getClass().getDeclaredField("sspAllocator");
    f.setAccessible(true);
    return (SSPAllocator) f.get(consumer);
  }

}
