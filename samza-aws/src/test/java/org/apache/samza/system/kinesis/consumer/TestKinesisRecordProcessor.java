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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;

import static org.mockito.Mockito.*;


public class TestKinesisRecordProcessor {
  private static final long MAX_WAIT_TIME_SHUTDOWN_RECEIVED_MS =
      KinesisRecordProcessor.POLL_INTERVAL_DURING_PARENT_SHARD_SHUTDOWN_MS + 1000;

  @Test
  public void testLifeCycleWithEvents() throws InterruptedException, ShutdownException, InvalidStateException,
                                               NoSuchFieldException, IllegalAccessException {
    testLifeCycleHelper(5);
  }

  @Test
  public void testLifeCycleWithNoEvents() throws InterruptedException, ShutdownException, InvalidStateException,
                                                 NoSuchFieldException, IllegalAccessException {
    testLifeCycleHelper(0);
  }

  private void testLifeCycleHelper(int numRecords) throws InterruptedException, ShutdownException,
                                                          InvalidStateException, NoSuchFieldException,
                                                          IllegalAccessException {
    String system = "kinesis";
    String stream = "stream";
    final CountDownLatch receivedShutdownLatch = new CountDownLatch(1);
    final CountDownLatch receivedRecordsLatch = new CountDownLatch(numRecords > 0 ? 1 : 0);

    KinesisRecordProcessorListener listener = new KinesisRecordProcessorListener() {
      @Override
      public void onReceiveRecords(SystemStreamPartition ssp, List<Record> records, long millisBehindLatest) {
        receivedRecordsLatch.countDown();
      }

      @Override
      public void onShutdown(SystemStreamPartition ssp) {
        receivedShutdownLatch.countDown();
      }
    };

    KinesisRecordProcessor processor =
        new KinesisRecordProcessor(new SystemStreamPartition(system, stream, new Partition(0)), listener);

    // Initialize the processor
    ExtendedSequenceNumber seqNum = new ExtendedSequenceNumber("0000");
    InitializationInput initializationInput =
        new InitializationInput().withShardId("shard-0000").withExtendedSequenceNumber(seqNum);
    processor.initialize(initializationInput);

    // Call processRecords on the processor
    List<Record> records = generateRecords(numRecords, Collections.singletonList(processor)).get(processor);

    // Verification steps

    // Verify there is a receivedRecords call to listener.
    Assert.assertTrue("Unable to receive records.", receivedRecordsLatch.getCount() == 0);

    if (numRecords > 0) {
      // Call checkpoint on last record
      processor.checkpoint(records.get(records.size() - 1).getSequenceNumber());
    }


    // Call shutdown (with ZOMBIE reason) on processor and verify that the processor calls shutdown on the listener.
    shutDownProcessor(processor, ShutdownReason.ZOMBIE);

    // Verify that the processor is shutdown.
    Assert.assertTrue("Unable to shutdown processor.", receivedShutdownLatch.getCount() == 0);
  }

  /**
   * Test the scenario where a processor instance is created for a shard and while it is processing records, it got
   * re-assigned to the same consumer. This results in a new processor instance owning the shard and this instance
   * could receive checkpoint calls for the records that are processed by the old processor instance. This test covers
   * the scenario where the new instance receives the checkpoint call while it is done with the initialization phase and
   * before it processed any records.
   */
  @Test
  public void testCheckpointAfterInit() throws InterruptedException, ShutdownException, InvalidStateException,
                                               NoSuchFieldException, IllegalAccessException {
    String system = "kinesis";
    String stream = "stream";
    final CountDownLatch receivedShutdownLatch = new CountDownLatch(1);

    KinesisRecordProcessorListener listener = new KinesisRecordProcessorListener() {
      @Override
      public void onReceiveRecords(SystemStreamPartition ssp, List<Record> records, long millisBehindLatest) {
      }

      @Override
      public void onShutdown(SystemStreamPartition ssp) {
        receivedShutdownLatch.countDown();
      }
    };

    KinesisRecordProcessor processor =
        new KinesisRecordProcessor(new SystemStreamPartition(system, stream, new Partition(0)), listener);

    // Initialize the processor
    ExtendedSequenceNumber seqNum = new ExtendedSequenceNumber("0000");
    InitializationInput initializationInput =
        new InitializationInput().withShardId("shard-0000").withExtendedSequenceNumber(seqNum);
    processor.initialize(initializationInput);

    // Call checkpoint. This checkpoint could have originally headed to the processor instance for the same shard but
    // due to reassignment a new processor instance is created.
    processor.checkpoint("1234567");


    // Call shutdown (with ZOMBIE reason) on processor and verify that the processor calls shutdown on the listener.
    shutDownProcessor(processor, ShutdownReason.ZOMBIE);

    // Verify that the processor is shutdown.
    Assert.assertTrue("Unable to shutdown processor.", receivedShutdownLatch.getCount() == 0);
  }

  @Test
  public void testShutdownDuringReshardWithEvents() throws InterruptedException, ShutdownException,
                                                           InvalidStateException, NoSuchFieldException,
                                                           IllegalAccessException {
    testShutdownDuringReshardHelper(5);
  }

  @Test
  public void testShutdownDuringReshardWithNoEvents() throws InterruptedException, ShutdownException,
                                                             InvalidStateException, NoSuchFieldException,
                                                             IllegalAccessException {
    testShutdownDuringReshardHelper(0);
  }

  private void testShutdownDuringReshardHelper(int numRecords)
      throws InterruptedException, ShutdownException, InvalidStateException, NoSuchFieldException,
             IllegalAccessException {
    String system = "kinesis";
    String stream = "stream";
    final CountDownLatch receivedShutdownLatch = new CountDownLatch(1);
    final CountDownLatch receivedRecordsLatch = new CountDownLatch(numRecords > 0 ? 1 : 0);

    KinesisRecordProcessorListener listener = new KinesisRecordProcessorListener() {
      @Override
      public void onReceiveRecords(SystemStreamPartition ssp, List<Record> records, long millisBehindLatest) {
        receivedRecordsLatch.countDown();
      }

      @Override
      public void onShutdown(SystemStreamPartition ssp) {
        receivedShutdownLatch.countDown();
      }
    };

    KinesisRecordProcessor processor =
        new KinesisRecordProcessor(new SystemStreamPartition(system, stream, new Partition(0)), listener);

    // Initialize the processor
    ExtendedSequenceNumber seqNum = new ExtendedSequenceNumber("0000");
    InitializationInput initializationInput =
        new InitializationInput().withShardId("shard-0000").withExtendedSequenceNumber(seqNum);
    processor.initialize(initializationInput);

    // Call processRecords on the processor
    List<Record> records = generateRecords(numRecords, Collections.singletonList(processor)).get(processor);

    // Verification steps

    // Verify there is a receivedRecords call to listener.
    Assert.assertTrue("Unable to receive records.", receivedRecordsLatch.getCount() == 0);

    // Call shutdown (with TERMINATE reason) on processor and verify that the processor does not call shutdown on the
    // listener until checkpoint is called for the last record consumed from shard.
    new Thread(() -> shutDownProcessor(processor, ShutdownReason.TERMINATE)).start();

    // If there are no records, the processor should shutdown immediately.
    if (numRecords == 0) {
      Assert.assertTrue("Unable to shutdown processor.",
          receivedShutdownLatch.await(MAX_WAIT_TIME_SHUTDOWN_RECEIVED_MS, TimeUnit.MILLISECONDS));
      return;
    }

    Assert.assertFalse("Processor shutdown too early.",
        receivedShutdownLatch.await(MAX_WAIT_TIME_SHUTDOWN_RECEIVED_MS, TimeUnit.MILLISECONDS));

    // Call checkpoint for the last but one record and the processor should still not call shutdown on listener.
    processor.checkpoint(records.get(records.size() - 2).getSequenceNumber());
    Assert.assertFalse("Processor shutdown too early.",
        receivedShutdownLatch.await(MAX_WAIT_TIME_SHUTDOWN_RECEIVED_MS, TimeUnit.MILLISECONDS));

    // Call checkpoint for the last record and the parent partition should be removed from mapper.
    processor.checkpoint(records.get(records.size() - 1).getSequenceNumber());
    Assert.assertTrue("Unable to shutdown processor.",
        receivedShutdownLatch.await(MAX_WAIT_TIME_SHUTDOWN_RECEIVED_MS, TimeUnit.MILLISECONDS));
  }

  static Map<KinesisRecordProcessor, List<Record>> generateRecords(int numRecordsPerShard,
      List<KinesisRecordProcessor> processors) throws ShutdownException, InvalidStateException {
    Map<KinesisRecordProcessor, List<Record>> processorRecordMap = new HashMap<>();
    processors.forEach(processor -> {
        try {
          // Create records and call process records
          IRecordProcessorCheckpointer checkpointer = Mockito.mock(IRecordProcessorCheckpointer.class);
          doNothing().when(checkpointer).checkpoint(anyString());
          doNothing().when(checkpointer).checkpoint();
          ProcessRecordsInput processRecordsInput = Mockito.mock(ProcessRecordsInput.class);
          when(processRecordsInput.getCheckpointer()).thenReturn(checkpointer);
          when(processRecordsInput.getMillisBehindLatest()).thenReturn(1000L);
          List<Record> inputRecords = createRecords(numRecordsPerShard);
          processorRecordMap.put(processor, inputRecords);
          when(processRecordsInput.getRecords()).thenReturn(inputRecords);
          processor.processRecords(processRecordsInput);
        } catch (ShutdownException | InvalidStateException ex) {
          throw new RuntimeException(ex);
        }
      });
    return processorRecordMap;
  }

  static void shutDownProcessor(KinesisRecordProcessor processor, ShutdownReason reason) {
    try {
      ShutdownInput shutdownInput = Mockito.mock(ShutdownInput.class);
      when(shutdownInput.getShutdownReason()).thenReturn(reason);
      when(shutdownInput.getCheckpointer()).thenReturn(getCheckpointer(processor));
      processor.shutdown(shutdownInput);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  static IRecordProcessorCheckpointer getCheckpointer(KinesisRecordProcessor processor)
      throws NoSuchFieldException, IllegalAccessException {
    Field f = processor.getClass().getDeclaredField("checkpointer");
    f.setAccessible(true);
    return (IRecordProcessorCheckpointer) f.get(processor);
  }

  private static List<Record> createRecords(int numRecords) {
    List<Record> records = new ArrayList<>(numRecords);
    Random rand = new Random();

    for (int i = 0; i < numRecords; i++) {
      String dataStr = "testData-" + System.currentTimeMillis();
      ByteBuffer data = ByteBuffer.wrap(dataStr.getBytes(StandardCharsets.UTF_8));
      String key = String.format("partitionKey-%d", rand.nextLong());
      String seqNum = String.format("%04d", 5 * i + 1);
      Record record = new Record()
          .withData(data)
          .withPartitionKey(key)
          .withSequenceNumber(seqNum)
          .withApproximateArrivalTimestamp(new Date());
      records.add(record);
    }
    return records;
  }
}