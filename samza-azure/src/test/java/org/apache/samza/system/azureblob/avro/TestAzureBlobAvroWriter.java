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

package org.apache.samza.system.azureblob.avro;

import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import org.apache.samza.system.azureblob.AzureBlobConfig;
import org.apache.samza.system.azureblob.compression.Compression;
import org.apache.samza.system.azureblob.compression.CompressionFactory;
import org.apache.samza.system.azureblob.compression.CompressionType;
import org.apache.samza.system.azureblob.producer.AzureBlobWriterMetrics;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.azureblob.utils.BlobMetadataGeneratorFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(PowerMockRunner.class)
@PrepareForTest({BlobContainerAsyncClient.class, BlockBlobAsyncClient.class, AzureBlobAvroWriter.class, AzureBlobOutputStream.class})
public class TestAzureBlobAvroWriter {
  private ThreadPoolExecutor threadPool;
  private OutgoingMessageEnvelope ome;
  private byte[] encodedRecord;
  private AzureBlobAvroWriter azureBlobAvroWriter;
  private DataFileWriter<Object> mockDataFileWriter;
  private AzureBlobOutputStream mockAzureBlobOutputStream;
  private BlockBlobAsyncClient mockBlockBlobAsyncClient;
  private Compression mockCompression;

  private final BlobMetadataGeneratorFactory blobMetadataGeneratorFactory = mock(BlobMetadataGeneratorFactory.class);
  private final Config blobMetadataGeneratorConfig = mock(Config.class);
  private static final String STREAM_NAME = "FAKE_STREAM";
  private static final String VALUE = "FAKE_VALUE";
  private static final String SYSTEM_NAME = "FAKE_SYSTEM";
  private static final int THRESHOLD = 100;
  private static final int INIT_SIZE = AzureBlobConfig.SYSTEM_INIT_BUFFER_SIZE_DEFAULT;

  private class SpecificRecordEvent extends org.apache.avro.specific.SpecificRecordBase
      implements org.apache.avro.specific.SpecificRecord {
    public final org.apache.avro.Schema schema = org.apache.avro.Schema.parse(
        "{\"type\":\"record\",\"name\":\"SpecificRecordEvent\",\"namespace\":\"org.apache.samza.events\",\"fields\":[]}");

    public org.apache.avro.Schema getSchema() {
      return schema;
    }

    public java.lang.Object get(int field) {
      return null;
    }

    public void put(int field, Object value) {}
  }

  private class GenericRecordEvent implements org.apache.avro.generic.GenericRecord {
    public final org.apache.avro.Schema schema = org.apache.avro.Schema.parse(
        "{\"type\":\"record\",\"name\":\"GenericRecordEvent\",\"namespace\":\"org.apache.samza.events\",\"fields\":[]}");

    public org.apache.avro.Schema getSchema() {
      return schema;
    }

    public java.lang.Object get(String key) {
      return null;
    }

    public java.lang.Object get(int field) {
      return null;
    }

    public void put(int field, Object value) {}
    public void put(String key, Object value) {}
  }

  // GenericFixed type is schema and encoded message
  private class GenericFixedEvent implements org.apache.avro.generic.GenericFixed {
    private final GenericRecordEvent record = new GenericRecordEvent();
    private final byte[] bytes;

    GenericFixedEvent(byte[] encoded) {
      bytes = encoded;
    }

    @Override
    public byte[] bytes() {
      return bytes;
    }

    @Override
    public Schema getSchema() {
      return record.getSchema();
    }
  }

  private OutgoingMessageEnvelope createOME(String streamName) {
    SystemStream systemStream = new SystemStream(SYSTEM_NAME, streamName);
    SpecificRecord record = new SpecificRecordEvent();
    return new OutgoingMessageEnvelope(systemStream, record);
  }

  private OutgoingMessageEnvelope createOMEGenericRecord(String streamName) {
    SystemStream systemStream = new SystemStream(SYSTEM_NAME, streamName);
    GenericRecord record = new GenericRecordEvent();
    return new OutgoingMessageEnvelope(systemStream, record);
  }

  private OutgoingMessageEnvelope createOMEGenericFixed(String streamName, byte[] encoded) {
    SystemStream systemStream = new SystemStream(SYSTEM_NAME, streamName);
    GenericFixed fixed = new GenericFixedEvent(encoded);
    return new OutgoingMessageEnvelope(systemStream, fixed);
  }

  @Before
  public void setup() throws Exception {
    threadPool = new ThreadPoolExecutor(1, 1, 60,  TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    ome = createOME("Topic1");

    encodedRecord = new byte[100];
    BlobContainerAsyncClient mockContainerAsyncClient = PowerMockito.mock(BlobContainerAsyncClient.class);
    mockDataFileWriter = (DataFileWriter<Object>) mock(DataFileWriter.class);
    mockAzureBlobOutputStream = mock(AzureBlobOutputStream.class);
    mockBlockBlobAsyncClient = PowerMockito.mock(BlockBlobAsyncClient.class);
    when(mockBlockBlobAsyncClient.getBlobUrl()).thenReturn("https://samza.blob.core.windows.net/fake-blob-url");

    mockCompression = CompressionFactory.getInstance().getCompression(CompressionType.GZIP);
    azureBlobAvroWriter =
        spy(new AzureBlobAvroWriter(mockContainerAsyncClient, mock(AzureBlobWriterMetrics.class), threadPool, THRESHOLD,
            60000, "test", mockDataFileWriter, mockAzureBlobOutputStream, mockBlockBlobAsyncClient,
            blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
            Long.MAX_VALUE, Long.MAX_VALUE, mockCompression, false, INIT_SIZE)); // keeping blob size and number of records unlimited
    doReturn(encodedRecord).when(azureBlobAvroWriter).encodeRecord((IndexedRecord) ome.getMessage());
  }
  @Test
  public void testWrite() throws Exception {
    int numberOfMessages = 10;
    for (int i = 0; i < numberOfMessages; ++i) {
      azureBlobAvroWriter.write(ome);
    }
    verify(mockDataFileWriter, times(numberOfMessages)).appendEncoded(ByteBuffer.wrap(encodedRecord));
    verify(mockAzureBlobOutputStream, times(numberOfMessages)).incrementNumberOfRecordsInBlob();
  }

  @Test
  public void testWriteGenericFixed() throws Exception {
    OutgoingMessageEnvelope omeGenericFixed = createOMEGenericFixed("Topic1", encodedRecord);
    int numberOfMessages = 10;
    for (int i = 0; i < numberOfMessages; ++i) {
      azureBlobAvroWriter.write(omeGenericFixed);
    }
    verify(mockDataFileWriter, times(numberOfMessages)).appendEncoded(ByteBuffer.wrap(encodedRecord));
    verify(mockAzureBlobOutputStream, times(numberOfMessages)).incrementNumberOfRecordsInBlob();
  }

  @Test
  public void testWriteGenericRecord() throws Exception {
    OutgoingMessageEnvelope omeGenericRecord = createOMEGenericRecord("Topic1");
    doReturn(encodedRecord).when(azureBlobAvroWriter).encodeRecord((IndexedRecord) omeGenericRecord.getMessage());
    int numberOfMessages = 10;
    for (int i = 0; i < numberOfMessages; ++i) {
      azureBlobAvroWriter.write(omeGenericRecord);
    }
    verify(mockDataFileWriter, times(numberOfMessages)).appendEncoded(ByteBuffer.wrap(encodedRecord));
    verify(mockAzureBlobOutputStream, times(numberOfMessages)).incrementNumberOfRecordsInBlob();
  }

  @Test
  public void testWriteByteArray() throws Exception {
    OutgoingMessageEnvelope omeEncoded = new OutgoingMessageEnvelope(new SystemStream(SYSTEM_NAME, "Topic1"), "randomString".getBytes());
    int numberOfMessages = 10;
    azureBlobAvroWriter.write(ome);
    for (int i = 0; i < numberOfMessages; ++i) {
      azureBlobAvroWriter.write(omeEncoded);
    }
    verify(mockDataFileWriter).appendEncoded(ByteBuffer.wrap(encodedRecord));
    verify(mockDataFileWriter, times(numberOfMessages)).appendEncoded(ByteBuffer.wrap((byte[]) omeEncoded.getMessage()));
    verify(mockAzureBlobOutputStream, times(numberOfMessages + 1)).incrementNumberOfRecordsInBlob(); // +1 to account for first ome which is not encoded
  }

  @Test(expected = IllegalStateException.class)
  public void testWriteByteArrayWithoutSchema() throws Exception {
    azureBlobAvroWriter =
        spy(new AzureBlobAvroWriter(PowerMockito.mock(BlobContainerAsyncClient.class), mock(AzureBlobWriterMetrics.class),
            threadPool, THRESHOLD, 60000, "test",
            null, null, null, blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
            1000, 100, mockCompression, false, INIT_SIZE));
    OutgoingMessageEnvelope omeEncoded = new OutgoingMessageEnvelope(new SystemStream(SYSTEM_NAME, "Topic1"), new byte[100]);
    azureBlobAvroWriter.write(omeEncoded);
  }

  @Test(expected = IOException.class)
  public void testWriteWhenDataFileWriterFails() throws Exception {
    doThrow(new IOException("Failed")).when(mockDataFileWriter).appendEncoded(ByteBuffer.wrap(encodedRecord));
    azureBlobAvroWriter.write(ome);
  }

  @Test
  public void testClose() throws Exception {
    azureBlobAvroWriter.close();
    verify(mockDataFileWriter).close();
  }

  @Test(expected = SamzaException.class)
  public void testCloseWhenDataFileWriterFails() throws Exception {
    doThrow(new IOException("Failed")).when(mockDataFileWriter).close();

    azureBlobAvroWriter.flush();
    azureBlobAvroWriter.close();
    verify(mockAzureBlobOutputStream, never()).close();
  }

  @Test(expected = RuntimeException.class)
  public void testCloseWhenOutputStreamFails() throws Exception {
    doThrow(new IOException("DataFileWriter failed")).when(mockDataFileWriter).close();
    doThrow(new RuntimeException("failed")).when(mockAzureBlobOutputStream).close();

    azureBlobAvroWriter.close();
  }

  @Test
  public void testFlush() throws Exception {
    azureBlobAvroWriter.flush();
    verify(mockDataFileWriter).flush();
  }

  @Test(expected = IOException.class)
  public void testFlushWhenDataFileWriterFails() throws Exception {
    doThrow(new IOException("Failed")).when(mockDataFileWriter).flush();
    azureBlobAvroWriter.flush();
  }

  @Test
  public void testNPEinFlush() throws Exception {
    // do not provide the dataFileWrite, azureBloboutputstream and blockblob client -- to force creation during first write
    azureBlobAvroWriter =
        spy(new AzureBlobAvroWriter(PowerMockito.mock(BlobContainerAsyncClient.class), mock(AzureBlobWriterMetrics.class), threadPool, THRESHOLD,
            60000, "test", null, null, null,
            blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
            Long.MAX_VALUE, Long.MAX_VALUE, mockCompression, false, INIT_SIZE)); // keeping blob size and number of records unlimited
    when(azureBlobAvroWriter.encodeRecord((IndexedRecord) ome.getMessage())).thenThrow(IllegalStateException.class);
    azureBlobAvroWriter.flush(); // No NPE because has null check for currentBlobWriterComponents
  }

  @Test
  public void testMaxBlobSizeExceeded() throws Exception {
    String blobUrlPrefix = "test";
    String blobNameRegex = "test/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/[0-9]{2}-[0-9]{2}-.{8}.avro.gz";
    long maxBlobSize = 1000;
    AzureBlobWriterMetrics mockMetrics = mock(AzureBlobWriterMetrics.class);
    BlobContainerAsyncClient mockContainerClient = PowerMockito.mock(BlobContainerAsyncClient.class);
    azureBlobAvroWriter = spy(new AzureBlobAvroWriter(mockContainerClient,
        mockMetrics, threadPool, THRESHOLD, 60000, blobUrlPrefix,
        null, null, null, blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
        maxBlobSize, 10, mockCompression, true, INIT_SIZE));

    DataFileWriter<Object> mockDataFileWriter1 = (DataFileWriter<Object>) mock(DataFileWriter.class);
    PowerMockito.whenNew(DataFileWriter.class).withAnyArguments().thenReturn(mockDataFileWriter1);

    BlobAsyncClient mockBlobAsyncClient1 = mock(BlobAsyncClient.class);
    doReturn(mockBlobAsyncClient1).when(mockContainerClient).getBlobAsyncClient(Matchers.matches(blobNameRegex));
    BlockBlobAsyncClient mockBlockBlobAsyncClient1 = mock(BlockBlobAsyncClient.class);
    doReturn(mockBlockBlobAsyncClient1).when(mockBlobAsyncClient1).getBlockBlobAsyncClient();

    AzureBlobOutputStream mockAzureBlobOutputStream1 = mock(AzureBlobOutputStream.class);
    PowerMockito.whenNew(AzureBlobOutputStream.class).withArguments(mockBlockBlobAsyncClient1, threadPool,
        mockMetrics, blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
        (long) 60000, THRESHOLD, mockCompression, INIT_SIZE).thenReturn(mockAzureBlobOutputStream1);
    when(mockAzureBlobOutputStream1.getSize()).thenReturn((long) maxBlobSize - 1);

    // first OME creates the first blob
    azureBlobAvroWriter.write(ome);

    OutgoingMessageEnvelope ome2 = createOME("Topic2");
    DataFileWriter<Object> mockDataFileWriter2 = (DataFileWriter<Object>) mock(DataFileWriter.class);
    PowerMockito.whenNew(DataFileWriter.class).withAnyArguments().thenReturn(mockDataFileWriter2);

    BlobAsyncClient mockBlobAsyncClient2 = mock(BlobAsyncClient.class);
    doReturn(mockBlobAsyncClient2).when(mockContainerClient).getBlobAsyncClient(Matchers.matches(blobNameRegex));
    BlockBlobAsyncClient mockBlockBlobAsyncClient2 = mock(BlockBlobAsyncClient.class);
    doReturn(mockBlockBlobAsyncClient2).when(mockBlobAsyncClient2).getBlockBlobAsyncClient();

    AzureBlobOutputStream mockAzureBlobOutputStream2 = mock(AzureBlobOutputStream.class);
    PowerMockito.whenNew(AzureBlobOutputStream.class).withArguments(mockBlockBlobAsyncClient2, threadPool,
        mockMetrics, blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
        (long) 60000, THRESHOLD, mockCompression, INIT_SIZE).thenReturn(mockAzureBlobOutputStream2);
    when(mockAzureBlobOutputStream2.getSize()).thenReturn((long) maxBlobSize - 1);

    // Second OME creates the second blob because maxBlobSize is 1000 and mockAzureBlobOutputStream.getSize is 999.
    azureBlobAvroWriter.write(ome2);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(mockContainerClient, times(2)).getBlobAsyncClient(argument.capture());
    argument.getAllValues().forEach(blobName -> {
      Assert.assertTrue(blobName.contains(blobUrlPrefix));
    });
    List<String> allBlobNames = argument.getAllValues();
    Assert.assertNotEquals(allBlobNames.get(0), allBlobNames.get(1));

    verify(mockDataFileWriter1).appendEncoded(ByteBuffer.wrap(encodeRecord((IndexedRecord) ome.getMessage())));
    verify(mockDataFileWriter2).appendEncoded(ByteBuffer.wrap(encodeRecord((IndexedRecord) ome2.getMessage())));

    verify(mockDataFileWriter1).create(((IndexedRecord) ome.getMessage()).getSchema(), mockAzureBlobOutputStream1);
    verify(mockDataFileWriter2).create(((IndexedRecord) ome2.getMessage()).getSchema(), mockAzureBlobOutputStream2);
  }

  @Test
  public void testRecordLimitExceeded() throws Exception {
    String blobUrlPrefix = "test";
    String blobNameRegex = "test/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}/[0-9]{2}-[0-9]{2}-.{8}.avro.gz";
    AzureBlobWriterMetrics mockMetrics = mock(AzureBlobWriterMetrics.class);
    long maxBlobSize = AzureBlobAvroWriter.DATAFILEWRITER_OVERHEAD + 1000;
    long maxRecordsPerBlob = 10;
    BlobContainerAsyncClient mockContainerClient = PowerMockito.mock(BlobContainerAsyncClient.class);
    azureBlobAvroWriter = spy(new AzureBlobAvroWriter(mockContainerClient,
        mockMetrics, threadPool, THRESHOLD, 60000, blobUrlPrefix,
        null, null, null, blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
        maxBlobSize, maxRecordsPerBlob, mockCompression, true, INIT_SIZE));

    DataFileWriter<Object> mockDataFileWriter1 = (DataFileWriter<Object>) mock(DataFileWriter.class);
    PowerMockito.whenNew(DataFileWriter.class).withAnyArguments().thenReturn(mockDataFileWriter1);

    BlobAsyncClient mockBlobAsyncClient1 = mock(BlobAsyncClient.class);
    doReturn(mockBlobAsyncClient1).when(mockContainerClient).getBlobAsyncClient(Matchers.matches(blobNameRegex));
    BlockBlobAsyncClient mockBlockBlobAsyncClient1 = mock(BlockBlobAsyncClient.class);
    doReturn(mockBlockBlobAsyncClient1).when(mockBlobAsyncClient1).getBlockBlobAsyncClient();

    AzureBlobOutputStream mockAzureBlobOutputStream1 = mock(AzureBlobOutputStream.class);
    PowerMockito.whenNew(AzureBlobOutputStream.class).withArguments(mockBlockBlobAsyncClient1, threadPool,
        mockMetrics, blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
        (long) 60000, THRESHOLD, mockCompression, INIT_SIZE).thenReturn(mockAzureBlobOutputStream1);
    when(mockAzureBlobOutputStream1.getSize()).thenReturn((long) 1);

    // first OME creates the first blob and 11th OME (ome2) creates the second blob.

    for (int i = 0; i < maxRecordsPerBlob; i++) {
      azureBlobAvroWriter.write(ome);
    }

    OutgoingMessageEnvelope ome2 = createOME("Topic2");
    DataFileWriter<Object> mockDataFileWriter2 = (DataFileWriter<Object>) mock(DataFileWriter.class);
    PowerMockito.whenNew(DataFileWriter.class).withAnyArguments().thenReturn(mockDataFileWriter2);

    BlobAsyncClient mockBlobAsyncClient2 = mock(BlobAsyncClient.class);
    doReturn(mockBlobAsyncClient2).when(mockContainerClient).getBlobAsyncClient(Matchers.matches(blobNameRegex));
    BlockBlobAsyncClient mockBlockBlobAsyncClient2 = mock(BlockBlobAsyncClient.class);
    doReturn(mockBlockBlobAsyncClient2).when(mockBlobAsyncClient2).getBlockBlobAsyncClient();

    AzureBlobOutputStream mockAzureBlobOutputStream2 = mock(AzureBlobOutputStream.class);
    PowerMockito.whenNew(AzureBlobOutputStream.class).withArguments(mockBlockBlobAsyncClient2, threadPool,
        mockMetrics, blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
        (long) 60000, THRESHOLD, mockCompression, INIT_SIZE).thenReturn(mockAzureBlobOutputStream2);
    when(mockAzureBlobOutputStream2.getSize()).thenReturn((long) 1);

    azureBlobAvroWriter.write(ome2);

    ArgumentCaptor<String> argument = ArgumentCaptor.forClass(String.class);
    verify(mockContainerClient, times(2)).getBlobAsyncClient(argument.capture());
    argument.getAllValues().forEach(blobName -> {
      Assert.assertTrue(blobName.contains(blobUrlPrefix));
    });
    List<String> allBlobNames = argument.getAllValues();
    Assert.assertNotEquals(allBlobNames.get(0), allBlobNames.get(1));

    verify(mockDataFileWriter1, times((int) maxRecordsPerBlob)).appendEncoded(ByteBuffer.wrap(encodeRecord((IndexedRecord) ome.getMessage())));
    verify(mockDataFileWriter2).appendEncoded(ByteBuffer.wrap(encodeRecord((IndexedRecord) ome2.getMessage())));

    verify(mockDataFileWriter1).create(((IndexedRecord) ome.getMessage()).getSchema(), mockAzureBlobOutputStream1);
    verify(mockDataFileWriter2).create(((IndexedRecord) ome2.getMessage()).getSchema(), mockAzureBlobOutputStream2);
  }

  @Test
  public void testMultipleBlobClose() throws Exception {
    String blobUrlPrefix = "test";
    long maxBlobSize = AzureBlobAvroWriter.DATAFILEWRITER_OVERHEAD + 1000;
    long maxRecordsPerBlob = 10;
    BlobContainerAsyncClient mockContainerClient = PowerMockito.mock(BlobContainerAsyncClient.class);
    azureBlobAvroWriter = spy(new AzureBlobAvroWriter(mockContainerClient,
        mock(AzureBlobWriterMetrics.class), threadPool, THRESHOLD, 60000, blobUrlPrefix,
        mockDataFileWriter, mockAzureBlobOutputStream, mockBlockBlobAsyncClient, blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
        maxBlobSize, maxRecordsPerBlob, mockCompression, false, INIT_SIZE));

    DataFileWriter<Object> mockDataFileWriter2 = mock(DataFileWriter.class);
    AzureBlobOutputStream mockAzureBlobOutputStream2 = mock(AzureBlobOutputStream.class);

    when(mockAzureBlobOutputStream.getSize()).thenReturn((long) 1);
    BlobAsyncClient mockBlobAsyncClient = mock(BlobAsyncClient.class);
    doReturn(mockBlobAsyncClient).when(mockContainerClient).getBlobAsyncClient(anyString());
    doReturn(mockBlockBlobAsyncClient).when(mockBlobAsyncClient).getBlockBlobAsyncClient();
    PowerMockito.whenNew(AzureBlobOutputStream.class).withAnyArguments().thenReturn(mockAzureBlobOutputStream2);
    PowerMockito.whenNew(DataFileWriter.class).withAnyArguments().thenReturn(mockDataFileWriter2);
    for (int i = 0; i <= maxRecordsPerBlob; i++) {
      azureBlobAvroWriter.write(ome);
    }
    // first OME creates the first blob and 11th OME creates the second blob.

    azureBlobAvroWriter.close();
    verify(mockDataFileWriter).close();
    verify(mockDataFileWriter2).close();
  }

  @Test
  public void testEncodeRecord() throws Exception {
    azureBlobAvroWriter = spy(new AzureBlobAvroWriter(PowerMockito.mock(BlobContainerAsyncClient.class),
        mock(AzureBlobWriterMetrics.class), threadPool, THRESHOLD,
        60000, "test", mockDataFileWriter, mockAzureBlobOutputStream, mockBlockBlobAsyncClient,
        blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, STREAM_NAME,
        Long.MAX_VALUE, Long.MAX_VALUE, mockCompression, false, INIT_SIZE));
    IndexedRecord record = new GenericRecordEvent();
    Assert.assertTrue(Arrays.equals(encodeRecord(record), azureBlobAvroWriter.encodeRecord(record)));
  }

  @Test
  public void testMultipleThreadWrites() throws Exception {
    Thread t1 = writeInThread(ome, azureBlobAvroWriter, 10);
    OutgoingMessageEnvelope ome2 = createOMEGenericRecord("TOPIC2");
    Thread t2 = writeInThread(ome2, azureBlobAvroWriter, 10);

    t1.start();
    t2.start();
    t1.join(60000);
    t2.join(60000);

    verify(mockDataFileWriter, times(10)).appendEncoded(ByteBuffer.wrap(encodedRecord));
    verify(mockDataFileWriter, times(10)).appendEncoded(ByteBuffer.wrap(encodeRecord((IndexedRecord) ome2.getMessage())));
    verify(mockAzureBlobOutputStream, times(20)).incrementNumberOfRecordsInBlob();
  }

  @Test
  public void testMultipleThreadWriteFlush() throws Exception {
    Thread t1 = writeInThread(ome, azureBlobAvroWriter, 10);
    Thread t2 = flushInThread(azureBlobAvroWriter);

    t1.start();
    t2.start();
    t1.join(60000);
    t2.join(60000);

    verify(mockDataFileWriter, times(10)).appendEncoded(ByteBuffer.wrap(encodedRecord));
    verify(mockAzureBlobOutputStream, times(10)).incrementNumberOfRecordsInBlob();
    verify(mockDataFileWriter).flush();
  }

  @Test
  public void testMultipleThreadWriteFlushInBoth() throws Exception {
    Thread t1 = writeFlushInThread(ome, azureBlobAvroWriter, 10);
    OutgoingMessageEnvelope ome2 = createOMEGenericRecord("TOPIC2");
    Thread t2 = writeFlushInThread(ome2, azureBlobAvroWriter, 10);

    t1.start();
    t2.start();
    t1.join(60000);
    t2.join(60000);

    verify(mockDataFileWriter, times(10)).appendEncoded(ByteBuffer.wrap(encodedRecord));
    verify(mockDataFileWriter, times(10)).appendEncoded(ByteBuffer.wrap(encodeRecord((IndexedRecord) ome2.getMessage())));
    verify(mockDataFileWriter, times(2)).flush();
    verify(mockAzureBlobOutputStream, times(20)).incrementNumberOfRecordsInBlob();
  }

  @Test
  public void testMultipleThreadWriteFlushFinallyClose() throws Exception {
    Thread t1 = writeFlushInThread(ome, azureBlobAvroWriter, 10);
    OutgoingMessageEnvelope ome2 = createOMEGenericRecord("TOPIC2");
    Thread t2 = writeFlushInThread(ome2, azureBlobAvroWriter, 10);

    t1.start();
    t2.start();
    t1.join(60000);
    t2.join(60000);
    azureBlobAvroWriter.close();

    verify(mockDataFileWriter, times(10)).appendEncoded(ByteBuffer.wrap(encodedRecord));
    verify(mockDataFileWriter, times(10)).appendEncoded(ByteBuffer.wrap(encodeRecord((IndexedRecord) ome2.getMessage())));
    verify(mockDataFileWriter, times(2)).flush();
    verify(mockDataFileWriter).close();
    verify(mockAzureBlobOutputStream, times(20)).incrementNumberOfRecordsInBlob();
  }

  private byte[] encodeRecord(IndexedRecord record) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Schema schema = record.getSchema();
    EncoderFactory encoderfactory = new EncoderFactory();
    BinaryEncoder encoder = encoderfactory.binaryEncoder(out, null);
    DatumWriter<IndexedRecord> writer;
    if (record instanceof SpecificRecord) {
      writer = new SpecificDatumWriter<>(schema);
    } else {
      writer = new GenericDatumWriter<>(schema);
    }
    writer.write(record, encoder);
    encoder.flush(); //encoder may buffer
    return out.toByteArray();
  }

  private Thread writeInThread(OutgoingMessageEnvelope ome, AzureBlobAvroWriter azureBlobAvroWriter,
      int numberOfSends) {
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < numberOfSends; i++) {
            azureBlobAvroWriter.write(ome);
          }
        } catch (IOException e) {
          throw new SamzaException(e);
        }
      }
    };
    return t;
  }

  private Thread flushInThread(AzureBlobAvroWriter azureBlobAvroWriter) {
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          azureBlobAvroWriter.flush();
        } catch (IOException e) {
          throw new SamzaException(e);
        }
      }
    };
    return t;
  }

  private Thread writeFlushInThread(OutgoingMessageEnvelope ome, AzureBlobAvroWriter azureBlobAvroWriter,
      int numberOfSends) {
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          for (int i = 0; i < numberOfSends; i++) {
            azureBlobAvroWriter.write(ome);
          }
          azureBlobAvroWriter.flush();
        } catch (IOException e) {
          throw new SamzaException(e);
        }
      }
    };
    return t;
  }
}
