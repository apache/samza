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

import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.samza.system.azureblob.compression.Compression;
import org.apache.samza.system.azureblob.producer.AzureBlobWriter;
import org.apache.samza.system.azureblob.producer.AzureBlobWriterMetrics;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Executor;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.azureblob.utils.BlobMetadataGeneratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements {@link org.apache.samza.system.azureblob.producer.AzureBlobWriter}
 * for writing avro records to Azure Blob Storage.
 *
 * It uses {@link org.apache.avro.file.DataFileWriter} to convert avro records it receives to byte[].
 * This byte[] is passed on to {@link org.apache.samza.system.azureblob.avro.AzureBlobOutputStream}.
 * AzureBlobOutputStream in turn uploads data to Storage as a blob.
 *
 * It also accepts encoded records as byte[] as long as the first OutgoingMessageEnvelope this writer receives
 * is a decoded record from which to get the schema and record type (GenericRecord vs SpecificRecord).
 * The subsequent encoded records are written directly to AzureBlobOutputStream without checking if they conform
 * to the schema. It is the responsibility of the user to ensure this. Failing to do so may result in an
 * unreadable avro blob.
 *
 * It expects all OutgoingMessageEnvelopes to be of the same schema.
 * To handle schema evolution (sending envelopes of different schema), this writer has to be closed and a new writer
 * has to be created. The first envelope of the new writer should contain a valid record to get schema from.
 * If used by AzureBlobSystemProducer, this is done through systemProducer.flush(source).
 *
 * Once closed this object can not be used.
 * This is a thread safe class.
 *
 * If the number of records or size of the current blob exceeds the specified limits then a new blob is created.
 */
public class AzureBlobAvroWriter implements AzureBlobWriter {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobAvroWriter.class);
  private static final String PUBLISHED_FILE_NAME_DATE_FORMAT = "yyyy/MM/dd/HH/mm-ss";
  private static final String BLOB_NAME_AVRO = "%s/%s.avro%s";
  private static final String BLOB_NAME_RANDOM_STRING_AVRO = "%s/%s-%s.avro%s";
  private static final SimpleDateFormat UTC_FORMATTER = buildUTCFormatter();

  // Avro's DataFileWriter has internal buffers and also adds metadata.
  // Based on the current default sizes of these buffers and metadata, the data overhead is a little less than 100KB
  // However, taking the overhead to be capped at 1MB to ensure enough room if the default values are increased.
  static final long DATAFILEWRITER_OVERHEAD = 1000000; // 1MB

  // currentBlobWriterComponents == null only for the first blob immediately after this AzureBlobAvroWriter object has been created.
  // rest of this object's lifecycle, currentBlobWriterComponents is not null.
  private BlobWriterComponents currentBlobWriterComponents = null;
  private final List<BlobWriterComponents> allBlobWriterComponents = new ArrayList<>();
  private Schema schema = null;
  // datumWriter == null only for the first blob immediately after this AzureBlobAvroWriter object has been created.
  // It is created from the schema taken from the first OutgoingMessageEnvelope. Hence the first OME has to be a decoded avro record.
  // For rest of this object's lifecycle, datumWriter is not null.
  private DatumWriter<IndexedRecord> datumWriter = null;
  private volatile boolean isClosed = false;

  private final Executor blobThreadPool;
  private final AzureBlobWriterMetrics metrics;
  private final int maxBlockFlushThresholdSize;
  private final long flushTimeoutMs;
  private final Compression compression;
  private final BlobContainerAsyncClient containerAsyncClient;
  private final String blobURLPrefix;
  private final long maxBlobSize;
  private final long maxRecordsPerBlob;
  private final boolean useRandomStringInBlobName;
  private final Object currentDataFileWriterLock = new Object();
  private volatile long recordsInCurrentBlob = 0;
  private BlobMetadataGeneratorFactory blobMetadataGeneratorFactory;
  private Config blobMetadataGeneratorConfig;
  private String streamName;

  public AzureBlobAvroWriter(BlobContainerAsyncClient containerAsyncClient, String blobURLPrefix,
      Executor blobThreadPool, AzureBlobWriterMetrics metrics,
      BlobMetadataGeneratorFactory blobMetadataGeneratorFactory, Config blobMetadataGeneratorConfig, String streamName,
      int maxBlockFlushThresholdSize, long flushTimeoutMs, Compression compression, boolean useRandomStringInBlobName,
      long maxBlobSize, long maxRecordsPerBlob) {

    this.blobThreadPool = blobThreadPool;
    this.metrics = metrics;
    this.maxBlockFlushThresholdSize = maxBlockFlushThresholdSize;
    this.flushTimeoutMs = flushTimeoutMs;
    this.compression = compression;
    this.containerAsyncClient = containerAsyncClient;
    this.blobURLPrefix = blobURLPrefix;
    this.useRandomStringInBlobName = useRandomStringInBlobName;
    this.maxBlobSize = maxBlobSize;
    this.maxRecordsPerBlob = maxRecordsPerBlob;
    this.blobMetadataGeneratorFactory = blobMetadataGeneratorFactory;
    this.blobMetadataGeneratorConfig = blobMetadataGeneratorConfig;
    this.streamName = streamName;
  }

  /**
   * This method expects the {@link org.apache.samza.system.OutgoingMessageEnvelope}
   * to contain a message which is a {@link org.apache.avro.generic.IndexedRecord} or an encoded record aka byte[].
   * If the record is already encoded, it will directly write the byte[] to the output stream without checking if it conforms to schema.
   * Else, it encodes the record and writes to output stream.
   * However, the first envelope should always be a record and not a byte[].
   * If the blocksize threshold crosses, it will upload the output stream contents as a block.
   * If the number of records in current blob or size of current blob exceed limits then a new blob is created.
   * Multi-threading and thread-safety:
   *  The underlying {@link org.apache.avro.file.DataFileWriter} is not thread-safe.
   *  For this reason, it is essential to wrap accesses to this object in a synchronized block.
   *  Method write(OutgoingMessageEnvelope) allows multiple threads to encode records as that operation is stateless but
   *  restricts access to the shared objects through the synchronized block.
   *  Concurrent access to shared objects is controlled through a common lock and synchronized block and hence ensures
   *  thread safety.
   * @param ome - OutgoingMessageEnvelope that contains the IndexedRecord (GenericRecord or SpecificRecord) or an encoded record as byte[]
   * @throws IOException when
   *       - OutgoingMessageEnvelope's message is not an IndexedRecord or
   *       - underlying dataFileWriter.append fails
   * @throws IllegalStateException when the first OutgoingMessageEnvelope's message is not a record.
   */
  @Override
  public void write(OutgoingMessageEnvelope ome) throws IOException {
    Optional<IndexedRecord> optionalIndexedRecord;
    byte[] encodedRecord;
    if (ome.getMessage() instanceof IndexedRecord) {
      optionalIndexedRecord = Optional.of((IndexedRecord) ome.getMessage());
      encodedRecord = encodeRecord((IndexedRecord) ome.getMessage());
    } else if (ome.getMessage() instanceof byte[]) {
      optionalIndexedRecord = Optional.empty();
      encodedRecord = (byte[]) ome.getMessage();
    } else {
      throw new IllegalArgumentException("AzureBlobAvroWriter only supports IndexedRecord and byte[].");
    }

    synchronized (currentDataFileWriterLock) {
      // if currentBlobWriterComponents is null, then it is the first blob of this AzureBlobAvroWriter object
      if (currentBlobWriterComponents == null || willCurrentBlobExceedSize(encodedRecord) || willCurrentBlobExceedRecordLimit()) {
        startNextBlob(optionalIndexedRecord);
      }
      currentBlobWriterComponents.dataFileWriter.appendEncoded(ByteBuffer.wrap(encodedRecord));
      recordsInCurrentBlob++;
      // incrementNumberOfRecordsInBlob should always be invoked every time appendEncoded above is invoked.
      // this is to count the number records in a blob and then use that count as a metadata of the blob.
      currentBlobWriterComponents.azureBlobOutputStream.incrementNumberOfRecordsInBlob();
    }
  }
  /**
   * This method flushes all records written in dataFileWriter to the underlying AzureBlobOutputStream.
   * dataFileWriter.flush then explicitly invokes flush of the AzureBlobOutputStream.
   * This in turn async uploads content of the output stream as a block and reinits the output stream.
   * AzureBlobOutputStream.flush is not ensured if dataFileWriter.flush fails.
   * In such a scenario, the current block is not uploaded and blocks uploaded so far are lost.
   * {@inheritDoc}
   * @throws IOException if underlying dataFileWriter.flush fails
   */
  @Override
  public void flush() throws IOException {
    synchronized (currentDataFileWriterLock) {
      if (!isClosed && currentBlobWriterComponents != null) {
        currentBlobWriterComponents.dataFileWriter.flush();
      }
    }
  }

  /**
   * This method closes all DataFileWriters and output streams associated with all the blobs created.
   * flush should be explicitly called before close.
   * {@inheritDoc}
   * @throws IllegalStateException when closing a closed writer
   * @throws SamzaException if underlying DataFileWriter.close fails
   */
  @Override
  public void close() {
    synchronized (currentDataFileWriterLock) {
      if (isClosed) {
        throw new IllegalStateException("Attempting to close an already closed AzureBlobAvroWriter");
      }
      allBlobWriterComponents.forEach(blobWriterComponents -> {
        try {
          closeDataFileWriter(blobWriterComponents.dataFileWriter, blobWriterComponents.azureBlobOutputStream,
              blobWriterComponents.blockBlobAsyncClient);
        } catch (IOException e) {
          throw new SamzaException(e);
        }
      });
      isClosed = true;
    }
  }

  @VisibleForTesting
  AzureBlobAvroWriter(BlobContainerAsyncClient containerAsyncClient, AzureBlobWriterMetrics metrics,
      Executor blobThreadPool, int maxBlockFlushThresholdSize, int flushTimeoutMs, String blobURLPrefix,
      DataFileWriter<IndexedRecord> dataFileWriter,
      AzureBlobOutputStream azureBlobOutputStream, BlockBlobAsyncClient blockBlobAsyncClient,
      BlobMetadataGeneratorFactory blobMetadataGeneratorFactory, Config blobMetadataGeneratorConfig, String streamName,
      long maxBlobSize, long maxRecordsPerBlob, Compression compression, boolean useRandomStringInBlobName) {
    if (dataFileWriter == null || azureBlobOutputStream == null || blockBlobAsyncClient == null) {
      this.currentBlobWriterComponents = null;
    } else {
      this.currentBlobWriterComponents =
          new BlobWriterComponents(dataFileWriter, azureBlobOutputStream, blockBlobAsyncClient);
    }
    this.allBlobWriterComponents.add(this.currentBlobWriterComponents);
    this.blobThreadPool = blobThreadPool;
    this.blobURLPrefix = blobURLPrefix;
    this.metrics = metrics;
    this.maxBlockFlushThresholdSize = maxBlockFlushThresholdSize;
    this.flushTimeoutMs = flushTimeoutMs;
    this.compression = compression;
    this.containerAsyncClient = containerAsyncClient;
    this.useRandomStringInBlobName = useRandomStringInBlobName;
    this.maxBlobSize = maxBlobSize;
    this.maxRecordsPerBlob = maxRecordsPerBlob;
    this.blobMetadataGeneratorFactory = blobMetadataGeneratorFactory;
    this.blobMetadataGeneratorConfig = blobMetadataGeneratorConfig;
    this.streamName = streamName;
  }

  @VisibleForTesting
  byte[] encodeRecord(IndexedRecord record) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Schema schema = record.getSchema();
    try {
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
    } catch (Exception e) {
      throw new SamzaException("Unable to serialize Avro record using schema within the record: " + schema.toString(), e);
    }
    return out.toByteArray();
  }

  private static SimpleDateFormat buildUTCFormatter() {
    SimpleDateFormat formatter = new SimpleDateFormat(PUBLISHED_FILE_NAME_DATE_FORMAT);
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    return formatter;
  }

  private void closeDataFileWriter(DataFileWriter dataFileWriter, AzureBlobOutputStream azureBlobOutputStream,
      BlockBlobAsyncClient blockBlobAsyncClient) throws IOException {
    try {
      LOG.info("Closing the blob: {}", blockBlobAsyncClient.getBlobUrl().toString());
      // dataFileWriter.close calls close of the azureBlobOutputStream associated with it.
      dataFileWriter.close();
    } catch (Exception e) {
      LOG.error("Exception occurred during DataFileWriter.close for blob  "
          + blockBlobAsyncClient.getBlobUrl()
          + ". All blocks uploaded so far for this blob will be discarded to avoid invalid blobs.");
      throw e;
    }
  }

  private void startNextBlob(Optional<IndexedRecord> optionalIndexedRecord) throws IOException {
    if (currentBlobWriterComponents != null) {
      LOG.info("Starting new blob as current blob size is "
          + currentBlobWriterComponents.azureBlobOutputStream.getSize()
          + " and max blob size is " + maxBlobSize
          + " or number of records is " + recordsInCurrentBlob
          + " and max records in blob is " + maxRecordsPerBlob);
      currentBlobWriterComponents.dataFileWriter.flush();
      currentBlobWriterComponents.azureBlobOutputStream.releaseBuffer();
      recordsInCurrentBlob = 0;
    }
    // datumWriter is null when AzureBlobAvroWriter is created but has not yet received a message.
    // optionalIndexedRecord is the first message in this case.
    if (datumWriter == null) {
      if (optionalIndexedRecord.isPresent()) {
        IndexedRecord record = optionalIndexedRecord.get();
        schema = record.getSchema();
        if (record instanceof SpecificRecord) {
          datumWriter = new SpecificDatumWriter<>(schema);
        } else {
          datumWriter = new GenericDatumWriter<>(schema);
        }
      } else {
        throw new IllegalStateException("Writing without schema setup.");
      }
    }
    String blobURL;
    if (useRandomStringInBlobName) {
      blobURL = String.format(BLOB_NAME_RANDOM_STRING_AVRO, blobURLPrefix,
          UTC_FORMATTER.format(System.currentTimeMillis()), UUID.randomUUID().toString().substring(0, 8),
          compression.getFileExtension());
    } else {
      blobURL = String.format(BLOB_NAME_AVRO, blobURLPrefix,
          UTC_FORMATTER.format(System.currentTimeMillis()), compression.getFileExtension());
    }
    LOG.info("Creating new blob: {}", blobURL);
    BlockBlobAsyncClient blockBlobAsyncClient = containerAsyncClient.getBlobAsyncClient(blobURL).getBlockBlobAsyncClient();

    DataFileWriter<IndexedRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    AzureBlobOutputStream azureBlobOutputStream;
    try {
      azureBlobOutputStream = new AzureBlobOutputStream(blockBlobAsyncClient, blobThreadPool, metrics,
          blobMetadataGeneratorFactory, blobMetadataGeneratorConfig,
          streamName, flushTimeoutMs, maxBlockFlushThresholdSize, compression);
    } catch (Exception e) {
      throw new SamzaException("Unable to create AzureBlobOutputStream", e);
    }
    dataFileWriter.create(schema, azureBlobOutputStream);
    dataFileWriter.setFlushOnEveryBlock(false);
    this.currentBlobWriterComponents = new BlobWriterComponents(dataFileWriter, azureBlobOutputStream, blockBlobAsyncClient);
    allBlobWriterComponents.add(this.currentBlobWriterComponents);
    LOG.info("Created new blob: {}", blobURL);
  }

  private boolean willCurrentBlobExceedSize(byte[] encodedRecord) {
    AzureBlobOutputStream azureBlobOutputStream = currentBlobWriterComponents.azureBlobOutputStream;
    return (azureBlobOutputStream.getSize() + encodedRecord.length + DATAFILEWRITER_OVERHEAD) > maxBlobSize;
  }

  private boolean willCurrentBlobExceedRecordLimit() {
    return (recordsInCurrentBlob + 1) > maxRecordsPerBlob;
  }

  /**
   * Holds the components needed to write to an Azure Blob
   * - including Avro's DataFileWriter, AzureBlobOutputStream and Azure's BlockBlobAsyncClient
   */
  private class BlobWriterComponents {
    final DataFileWriter<IndexedRecord> dataFileWriter;
    final AzureBlobOutputStream azureBlobOutputStream;
    final BlockBlobAsyncClient blockBlobAsyncClient;

    public BlobWriterComponents(DataFileWriter dataFileWriter, AzureBlobOutputStream azureBlobOutputStream,
        BlockBlobAsyncClient blockBlobAsyncClient) {
      Preconditions.checkNotNull(dataFileWriter, "DataFileWriter can not be null when creating WriterComponents for an Azure Blob.");
      Preconditions.checkNotNull(azureBlobOutputStream, "AzureBlobOutputStream can not be null when creating WriterComponents for an Azure Blob.");
      Preconditions.checkNotNull(blockBlobAsyncClient, "BlockBlobAsyncClient can not be null when creating WriterComponents for an Azure Blob.");
      this.dataFileWriter = dataFileWriter;
      this.azureBlobOutputStream = azureBlobOutputStream;
      this.blockBlobAsyncClient = blockBlobAsyncClient;
    }
  }
}
