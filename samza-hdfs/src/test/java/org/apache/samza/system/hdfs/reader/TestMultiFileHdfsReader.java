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

package org.apache.samza.system.hdfs.reader;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.avro.generic.GenericRecord;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestMultiFileHdfsReader {
  private static final String FIELD_1 = "field1";
  private static final String FIELD_2 = "field2";
  private static final String WORKING_DIRECTORY = TestMultiFileHdfsReader.class.getResource("/reader").getPath();
  private static final String AVRO_FILE_1 = WORKING_DIRECTORY + "/TestMultiFileHdfsReader-01.avro";
  private static final String AVRO_FILE_2 = WORKING_DIRECTORY + "/TestMultiFileHdfsReader-02.avro";
  private static final String AVRO_FILE_3 = WORKING_DIRECTORY + "/TestMultiFileHdfsReader-03.avro";
  private static String[] descriptors = {AVRO_FILE_1, AVRO_FILE_2, AVRO_FILE_3};
  private static final int NUM_EVENTS = 100;

  @BeforeClass
  public static void writeAvroEvents()
    throws Exception {
    TestAvroFileHdfsReader.writeTestEventsToFile(AVRO_FILE_1, NUM_EVENTS);
    TestAvroFileHdfsReader.writeTestEventsToFile(AVRO_FILE_2, NUM_EVENTS);
    TestAvroFileHdfsReader.writeTestEventsToFile(AVRO_FILE_3, NUM_EVENTS);
  }

  @Test
  public void testSequentialRead()
    throws Exception {
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
    MultiFileHdfsReader multiReader = new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp, Arrays.asList(descriptors), "0:0");
    int index = 0;
    while (multiReader.hasNext()) {
      GenericRecord record = (GenericRecord) multiReader.readNext().getMessage();
      Assert.assertEquals(index % NUM_EVENTS, record.get(FIELD_1));
      Assert.assertEquals("string_" + (index % NUM_EVENTS), record.get(FIELD_2).toString());
      index++;
    }
    Assert.assertEquals(3 * NUM_EVENTS, index);
    multiReader.close();
  }

  @Test
  public void testReaderReopen()
    throws Exception {
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));

    // read until the middle of the first file
    MultiFileHdfsReader multiReader = new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp,
      Arrays.asList(descriptors), "0:0");
    int index = 0;
    String offset = "0:0";
    for (; index < NUM_EVENTS / 2; index++) {
      IncomingMessageEnvelope envelope = multiReader.readNext();
      GenericRecord record = (GenericRecord) envelope.getMessage();
      Assert.assertEquals(index % NUM_EVENTS, record.get(FIELD_1));
      Assert.assertEquals("string_" + (index % NUM_EVENTS), record.get(FIELD_2).toString());
      offset = envelope.getOffset();
    }
    multiReader.close();

    // read until the middle of the second file
    multiReader = new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp,
      Arrays.asList(descriptors), offset);
    multiReader.readNext(); // notAValidEvent one duplicate event
    for (; index < NUM_EVENTS + NUM_EVENTS / 2; index++) {
      IncomingMessageEnvelope envelope = multiReader.readNext();
      GenericRecord record = (GenericRecord) envelope.getMessage();
      Assert.assertEquals(index % NUM_EVENTS, record.get(FIELD_1));
      Assert.assertEquals("string_" + (index % NUM_EVENTS), record.get(FIELD_2).toString());
      offset = envelope.getOffset();
    }
    multiReader.close();

    // read the rest of all files
    multiReader = new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp,
      Arrays.asList(descriptors), offset);
    multiReader.readNext(); // notAValidEvent one duplicate event
    while (multiReader.hasNext()) {
      IncomingMessageEnvelope envelope = multiReader.readNext();
      GenericRecord record = (GenericRecord) envelope.getMessage();
      Assert.assertEquals(index % NUM_EVENTS, record.get(FIELD_1));
      Assert.assertEquals("string_" + (index % NUM_EVENTS), record.get(FIELD_2).toString());
      index++;
      offset = envelope.getOffset();
    }
    Assert.assertEquals(3 * NUM_EVENTS, index);
    multiReader.close();

    // reopen with the offset of the last record
    multiReader = new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp,
      Arrays.asList(descriptors), offset);
    multiReader.readNext(); // notAValidEvent one duplicate event
    Assert.assertFalse(multiReader.hasNext());
    multiReader.close();
  }

  @Test(expected = SamzaException.class)
  public void testOutOfRangeSingleFileOffset() {
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
    new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp,
      Arrays.asList(descriptors), "0:1000000&0");
    Assert.fail();
  }

  @Test(expected = SamzaException.class)
  public void testOutOfRangeFileIndex() {
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
    new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp,
      Arrays.asList(descriptors), "3:0");
    Assert.fail();
  }

  @Test(expected = SamzaException.class)
  public void testInvalidPartitionDescriptor() {
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
    new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp,
      new ArrayList<>(), "0:0");
    Assert.fail();
  }

  @Test
  public void testReconnect() {
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
    MultiFileHdfsReader multiReader = new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp, Arrays.asList(descriptors), "0:0");
    // first read a few events, and then reconnect
    for (int i = 0; i < NUM_EVENTS / 2; i++) {
      multiReader.readNext();
    }
    IncomingMessageEnvelope envelope = multiReader.readNext();
    multiReader.reconnect();
    IncomingMessageEnvelope envelopeAfterReconnect = multiReader.readNext();
    Assert.assertEquals(envelope, envelopeAfterReconnect);
    multiReader.close();
  }

  @Test(expected = SamzaException.class)
  public void testReachingMaxReconnect() {
    int numMaxRetries = 3;
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
    MultiFileHdfsReader multiReader = new MultiFileHdfsReader(HdfsReaderFactory.ReaderType.AVRO, ssp, Arrays.asList(descriptors), "0:0", numMaxRetries);
    // first read a few events, and then reconnect
    for (int i = 0; i < NUM_EVENTS / 2; i++) {
      multiReader.readNext();
    }
    for (int i = 0; i < numMaxRetries; i++) {
      IncomingMessageEnvelope envelope = multiReader.readNext();
      multiReader.reconnect();
      IncomingMessageEnvelope envelopeAfterReconnect = multiReader.readNext();
      Assert.assertEquals(envelope, envelopeAfterReconnect);
    }
    multiReader.readNext();
    multiReader.reconnect();
    Assert.fail();
  }
}
