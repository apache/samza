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


import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestAvroFileHdfsReader {

  private static final String FIELD_1 = "field1";
  private static final String FIELD_2 = "field2";
  private static final String WORKING_DIRECTORY = TestAvroFileHdfsReader.class.getResource("/reader").getPath();
  private static final String AVRO_FILE = WORKING_DIRECTORY + "/TestAvroFileHdfsReader-01.avro";
  private static final int NUM_EVENTS = 500;

  public static void writeTestEventsToFile(String path, int numEvents)
    throws Exception {
    Schema schema = Schema.parse(TestAvroFileHdfsReader.class.getResourceAsStream("/reader/TestEvent.avsc"));
    File file = new File(path);
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer);
    dataFileWriter.create(schema, file);
    for (int i = 0; i < numEvents; i++) {
      GenericRecord datum = new GenericData.Record(schema);
      datum.put(FIELD_1, i);
      datum.put(FIELD_2, "string_" + i);
      dataFileWriter.append(datum);
    }
    dataFileWriter.close();
  }

  @BeforeClass
  public static void writeAvroEvents() throws Exception {
    writeTestEventsToFile(AVRO_FILE, NUM_EVENTS);
  }

  @Test
  public void testSequentialRead() throws Exception {
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
    SingleFileHdfsReader reader = new AvroFileHdfsReader(ssp);
    reader.open(AVRO_FILE, "0");
    int index = 0;
    while (reader.hasNext()) {
      GenericRecord record = (GenericRecord) reader.readNext().getMessage();
      Assert.assertEquals(index, record.get(FIELD_1));
      Assert.assertEquals("string_" + index, record.get(FIELD_2).toString());
      index++;
    }
    Assert.assertEquals(NUM_EVENTS, index);
    reader.close();
  }

  @Test
  public void testFileReopen() throws Exception {
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
    SingleFileHdfsReader reader = new AvroFileHdfsReader(ssp);
    reader.open(AVRO_FILE, "0");
    int index = 0;
    for (;index < NUM_EVENTS / 2; index++) {
      GenericRecord record = (GenericRecord) reader.readNext().getMessage();
      Assert.assertEquals(index, record.get(FIELD_1));
      Assert.assertEquals("string_" + index, record.get(FIELD_2).toString());
    }
    String offset = reader.nextOffset();
    reader.close();
    reader = new AvroFileHdfsReader(ssp);
    reader.open(AVRO_FILE, offset);
    for (;index < NUM_EVENTS; index++) {
      GenericRecord record = (GenericRecord) reader.readNext().getMessage();
      Assert.assertEquals(index, record.get(FIELD_1));
      Assert.assertEquals("string_" + index, record.get(FIELD_2).toString());
    }
    Assert.assertEquals(NUM_EVENTS, index);
    reader.close();
  }

  @Test
  public void testRandomRead() throws Exception {
    SystemStreamPartition ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
    SingleFileHdfsReader reader = new AvroFileHdfsReader(ssp);
    reader.open(AVRO_FILE, "0");
    for (int i = 0;i < NUM_EVENTS / 2; i++) {
      reader.readNext();
    }
    String offset = reader.nextOffset();
    IncomingMessageEnvelope envelope = reader.readNext();
    Assert.assertEquals(offset, envelope.getOffset());

    GenericRecord record1 = (GenericRecord) envelope.getMessage();

    for (int i = 0; i < 5; i++) reader.readNext();

    // seek to the offset within the same reader
    reader.seek(offset);
    Assert.assertEquals(offset, reader.nextOffset());
    envelope = reader.readNext();
    Assert.assertEquals(offset, envelope.getOffset());
    GenericRecord record2 = (GenericRecord) envelope.getMessage();
    Assert.assertEquals(record1, record2);
    reader.close();

    // open a new reader and initialize it with the offset
    reader = new AvroFileHdfsReader(ssp);
    reader.open(AVRO_FILE, offset);
    envelope = reader.readNext();
    Assert.assertEquals(offset, envelope.getOffset());
    GenericRecord record3 = (GenericRecord) envelope.getMessage();
    Assert.assertEquals(record1, record3);
    reader.close();
  }

  @Test
  public void testOffsetComparator() {
    Assert.assertEquals(-1, AvroFileHdfsReader.offsetComparator("0", "1452"));
    Assert.assertEquals(-1, AvroFileHdfsReader.offsetComparator("2001@3", "2001@4"));
    Assert.assertEquals(-1, AvroFileHdfsReader.offsetComparator("2001@4", "2010@1"));
    Assert.assertEquals(-1, AvroFileHdfsReader.offsetComparator("2001@3", "2011@3"));
    Assert.assertEquals(-1, AvroFileHdfsReader.offsetComparator("2001", "2001@4"));
    Assert.assertEquals(-1, AvroFileHdfsReader.offsetComparator("2001", "2010@1"));
    Assert.assertEquals(-1, AvroFileHdfsReader.offsetComparator("2001@3", "2010"));
    Assert.assertEquals(1, AvroFileHdfsReader.offsetComparator("1984", "0"));
    Assert.assertEquals(1, AvroFileHdfsReader.offsetComparator("1984@2", "1984@1"));
    Assert.assertEquals(1, AvroFileHdfsReader.offsetComparator("14341@2", "1984@2"));
    Assert.assertEquals(1, AvroFileHdfsReader.offsetComparator("14341@1", "1984@10"));
    Assert.assertEquals(1, AvroFileHdfsReader.offsetComparator("14341", "1984@10"));
    Assert.assertEquals(1, AvroFileHdfsReader.offsetComparator("14341@1", "1984"));
    Assert.assertEquals(0, AvroFileHdfsReader.offsetComparator("1989", "1989"));
    Assert.assertEquals(0, AvroFileHdfsReader.offsetComparator("1989@0", "1989"));
    Assert.assertEquals(0, AvroFileHdfsReader.offsetComparator("1989", "1989@0"));
    Assert.assertEquals(0, AvroFileHdfsReader.offsetComparator("0", "0"));
    Assert.assertEquals(0, AvroFileHdfsReader.offsetComparator("1989@1", "1989@1"));
  }

  @Test(expected = Exception.class)
  public void testOffsetComparator_InvalidInput() {
    AvroFileHdfsReader.offsetComparator("1982,13", "1930,1");
  }
}
