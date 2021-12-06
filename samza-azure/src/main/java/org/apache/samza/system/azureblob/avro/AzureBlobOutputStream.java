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

import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import org.apache.samza.AzureException;
import org.apache.samza.system.azureblob.compression.Compression;
import org.apache.samza.system.azureblob.producer.AzureBlobWriterMetrics;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.samza.config.Config;
import org.apache.samza.system.azureblob.utils.BlobMetadataContext;
import org.apache.samza.system.azureblob.utils.BlobMetadataGenerator;
import org.apache.samza.system.azureblob.utils.BlobMetadataGeneratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;


/**
 * This class extends {@link java.io.OutputStream} and uses {@link java.io.ByteArrayOutputStream}
 * for caching the write calls till upload is not called.
 *
 * It asynchronously uploads the blocks and waits on them to finish at close.
 * The blob is persisted at close.
 *
 * flush must be explicitly called before close.
 * Any writes after a flush and before a close will be lost if no flush is called just before close.
 * Once closed this object can not be used.
 *
 * releaseBuffer releases the underlying buffer i.e ByteArrayOutputStream which holds the data written until it is flushed.
 * flush must be explicitly called prior to releaseBuffer else all data written
 * since the beginning/previous flush will be lost.
 * No data can be written after releaseBuffer, flush after releaseBuffer is a no-op
 * and close must still be invoked to wait for all pending uploads to finish and persist the blob.
 * releaseBuffer is optional and maybe called after its last flush and before close (which might happen much later),
  * so as to reduce the overall memory footprint. close can not replace releaseBuffer as it is a blocking call.
 *
 * This library is thread safe.
 */
public class AzureBlobOutputStream extends OutputStream {

  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobOutputStream.class);
  private static final int MAX_BLOCKS_IN_AZURE_BLOB = 50000;
  private final long flushTimeoutMs;
  private final BlockBlobAsyncClient blobAsyncClient;
  private final Executor blobThreadPool;
  private Optional<ByteArrayOutputStream> byteArrayOutputStream;
  // All the block Names should be explicitly present in the blockList during CommitBlockList,
  // even if stageBlock is a blocking call.
  private final ArrayList<String> blockList;
  private final Set<CompletableFuture<Void>> pendingUpload = ConcurrentHashMap.newKeySet();
  private final int maxBlockFlushThresholdSize;
  private final AzureBlobWriterMetrics metrics;
  private final Compression compression;

  private volatile boolean isClosed = false;
  private long totalUploadedBlockSize = 0;
  private long totalNumberOfRecordsInBlob = 0;
  private int blockNum;
  private final BlobMetadataGeneratorFactory blobMetadataGeneratorFactory;
  private final Config blobMetadataGeneratorConfig;
  private String streamName;

  /**
   *
   * @param blobAsyncClient Client to communicate with Azure Blob Storage.
   * @param blobThreadPool threads to be used for uploading blocks to Azure Blob Storage.
   * @param metrics needed for emitting metrics about bytes written, blocks uploaded, blobs committed.
   * @param blobMetadataGeneratorFactory impl of {@link org.apache.samza.system.azureblob.utils.BlobMetadataGeneratorFactory}
   *                                   to be used for generating metadata properties for a blob
   * @param streamName name of the stream to which the blob generated corresponds to. Used in metadata properties.
   * @param flushTimeoutMs timeout for uploading a block
   * @param maxBlockFlushThresholdSize max block size
   * @param compression type of compression to be used before uploading a block
   */
  public AzureBlobOutputStream(BlockBlobAsyncClient blobAsyncClient, Executor blobThreadPool, AzureBlobWriterMetrics metrics,
      BlobMetadataGeneratorFactory blobMetadataGeneratorFactory, Config blobMetadataGeneratorConfig, String streamName,
      long flushTimeoutMs, int maxBlockFlushThresholdSize, Compression compression) {
    this(blobAsyncClient, blobThreadPool, metrics, blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, streamName,
        flushTimeoutMs, maxBlockFlushThresholdSize,
        new ByteArrayOutputStream(maxBlockFlushThresholdSize), compression);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void write(int b) {
    if (!byteArrayOutputStream.isPresent()) {
      throw new IllegalStateException("Internal Buffer must have been released earlier for blob " + blobAsyncClient.getBlobUrl().toString());
    }

    if (byteArrayOutputStream.get().size() + 1 > maxBlockFlushThresholdSize) {
      uploadBlockAsync();
    }
    byteArrayOutputStream.get().write(b);
    metrics.updateWriteByteMetrics(1);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void write(byte[] b, int off, int len) {
    if (!byteArrayOutputStream.isPresent()) {
      throw new IllegalStateException("Internal Buffer must have been released earlier for blob " + blobAsyncClient.getBlobUrl().toString());
    }

    int remainingBytes = len;
    int offset = off;
    while (remainingBytes > 0) {
      int bytesToWrite = Math.min(maxBlockFlushThresholdSize - byteArrayOutputStream.get().size(), remainingBytes);
      byteArrayOutputStream.get().write(b, offset, bytesToWrite);
      offset += bytesToWrite;
      remainingBytes -= bytesToWrite;
      if (byteArrayOutputStream.get().size() >= maxBlockFlushThresholdSize) {
        uploadBlockAsync();
      }
    }
    metrics.updateWriteByteMetrics(len);
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void flush() {
    if (byteArrayOutputStream.isPresent()) {
      uploadBlockAsync();
    }
  }

  /**
   * This api waits for all pending upload (stageBlock task) futures to finish.
   * It then synchronously commits the list of blocks to persist the actual blob on storage.
   * Note: this method does not invoke flush and flush has to be explicitly called before close.
   * @throws IllegalStateException when
   *       - when closing an already closed stream
   * @throws RuntimeException when
   *       - byteArrayOutputStream.close fails or
   *       - any of the pending uploads fails or
   *       - blob's commitBlockList fails
   * throws ClassNotFoundException or IllegalAccessException or InstantiationException
   *       - while creating an instance of BlobMetadataGenerator
   */
  @Override
  public synchronized void close() {
    if (isClosed) {
      LOG.info("{}: already closed", blobAsyncClient.getBlobUrl().toString());
      return;
    }

    LOG.info("{}: Close", blobAsyncClient.getBlobUrl().toString());
    try {
      if (byteArrayOutputStream.isPresent()) {
        byteArrayOutputStream.get().close();
      }
      if (blockList.size() == 0) {
        return;
      }
      CompletableFuture<Void> future =
          CompletableFuture.allOf(pendingUpload.toArray(new CompletableFuture[0]));

      LOG.info("Closing blob: {} PendingUpload:{} ", blobAsyncClient.getBlobUrl().toString(), pendingUpload.size());

      future.get((long) flushTimeoutMs, TimeUnit.MILLISECONDS);
      LOG.info("For blob: {} committing blockList size:{}", blobAsyncClient.getBlobUrl().toString(), blockList.size());
      metrics.updateAzureCommitMetrics();
      BlobMetadataGenerator blobMetadataGenerator = getBlobMetadataGenerator();
      commitBlob(blockList, blobMetadataGenerator.getBlobMetadata(new BlobMetadataContext(streamName, totalUploadedBlockSize, totalNumberOfRecordsInBlob)));
    } catch (Exception e) {
      String msg = String.format("Close blob %s failed with exception. Total pending sends %d",
          blobAsyncClient.getBlobUrl().toString(), pendingUpload.size());
      throw new AzureException(msg, e);
    } finally {
      clearAndMarkClosed();
    }
  }

  /**
   * Returns the size of the blob so far including data in the uploaded blocks and data currently in buffer.
   * @return data written since the beginning
   */
  public synchronized long getSize() {
    return byteArrayOutputStream.isPresent() ? byteArrayOutputStream.get().size() + totalUploadedBlockSize : totalUploadedBlockSize;
  }

  /**
   * Releases the underlying buffer i.e; ByteArrayOutputStream.
   * flush must be explicitly called prior to releaseBuffer else all data written
   * since the beginning/previous flush will be lost.
   * No data can be written after releaseBuffer, flush after releaseBuffer is a no-op
   * and close must still be invoked to wait for all pending uploads to finish and persist the blob.
   * This is optional and can be used to release memory.
   * @throws IOException if ByteArrayOutputStream.close fails
   */
  public synchronized void releaseBuffer() throws IOException {
    if (byteArrayOutputStream.isPresent()) {
      byteArrayOutputStream.get().close();
      byteArrayOutputStream = Optional.empty();
      LOG.info("Internal buffer has been released for blob " + blobAsyncClient.getBlobUrl().toString()
          + ". Writes are no longer entertained.");
    }
  }

  /**
   * This method is to be used for tracking the number of records written to the outputstream.
   * However, since records are written in chunks through write(byte[],int,int) method,
   * it is possible that all records are not completely written until flush is invoked.
   *
   * Additionally, the count of number of records is intended to be used only as part of
   * blob's metadata at blob commit time which happens at close.
   * Thus, the totalNumberOfRecordsInBlob is not fetched until close method.
   * Since flush is called before close, this totalNumberOfRecordsInBlob is accurate.
   */
  public synchronized void incrementNumberOfRecordsInBlob() {
    totalNumberOfRecordsInBlob++;
  }

  @VisibleForTesting
  AzureBlobOutputStream(BlockBlobAsyncClient blobAsyncClient, Executor blobThreadPool, AzureBlobWriterMetrics metrics,
      BlobMetadataGeneratorFactory blobMetadataGeneratorFactory, Config blobMetadataGeneratorConfig, String streamName,
      long flushTimeoutMs, int maxBlockFlushThresholdSize,
      ByteArrayOutputStream byteArrayOutputStream, Compression compression) {
    this.byteArrayOutputStream = Optional.of(byteArrayOutputStream);
    this.blobAsyncClient = blobAsyncClient;
    blockList = new ArrayList<>();
    blockNum = 0;
    this.blobThreadPool = blobThreadPool;
    this.flushTimeoutMs = flushTimeoutMs;
    this.maxBlockFlushThresholdSize = maxBlockFlushThresholdSize;
    this.metrics = metrics;
    this.compression = compression;
    this.blobMetadataGeneratorFactory = blobMetadataGeneratorFactory;
    this.blobMetadataGeneratorConfig = blobMetadataGeneratorConfig;
    this.streamName = streamName;
  }

  // SAMZA-2476 stubbing BlockBlobAsyncClient.commitBlockListWithResponse was causing flaky tests.
  @VisibleForTesting
  void commitBlob(ArrayList<String> blockList, Map<String, String> blobMetadata) {
    blobAsyncClient.commitBlockListWithResponse(blockList, null, blobMetadata, null, null).block();
  }

  // SAMZA-2476 stubbing BlockBlobAsyncClient.stageBlock was causing flaky tests.
  @VisibleForTesting
  void stageBlock(String blockIdEncoded, ByteBuffer outputStream, int blockSize) throws InterruptedException {
    invokeBlobClientStageBlock(blockIdEncoded, outputStream, blockSize).subscribeOn(Schedulers.boundedElastic()).block(
        Duration.ofMillis(flushTimeoutMs));
  }

  @VisibleForTesting
  Mono<Void> invokeBlobClientStageBlock(String blockIdEncoded, ByteBuffer outputStream, int blockSize) {
    return blobAsyncClient.stageBlock(blockIdEncoded, Flux.just(outputStream), blockSize);
  }

  // blockList cleared makes it hard to test close
  @VisibleForTesting
  void clearAndMarkClosed() {
    blockList.clear();
    pendingUpload.stream().forEach(future -> future.cancel(true));
    pendingUpload.clear();
    isClosed = true;
  }

  @VisibleForTesting
  BlobMetadataGenerator getBlobMetadataGenerator() throws Exception {
    return blobMetadataGeneratorFactory.getBlobMetadataGeneratorInstance(blobMetadataGeneratorConfig);
  }

  /**
   * This api will async upload the outputstream into block using stageBlocks,
   * reint outputstream
   * and add the operation to future.
   * @throws RuntimeException when
   *            - blob's stageBlock fails after MAX_ATTEMPTs
   *            - number of blocks exceeds MAX_BLOCKS_IN_AZURE_BLOB
   */
  private synchronized void uploadBlockAsync() {
    if (!byteArrayOutputStream.isPresent()) {
      return;
    }
    long size = byteArrayOutputStream.get().size();
    if (size == 0) {
      return;
    }
    LOG.info("Blob: {} uploadBlock. Size:{}", blobAsyncClient.getBlobUrl().toString(), size);

    // Azure sdk requires block Id to be encoded and all blockIds of a blob to be of the same length
    // also, a block blob can have upto 50,000 blocks, hence using a 5 digit block id.
    String blockId = String.format("%05d", blockNum);
    String blockIdEncoded = Base64.getEncoder().encodeToString(blockId.getBytes());
    blockList.add(blockIdEncoded);
    byte[] localByte = byteArrayOutputStream.get().toByteArray();
    byteArrayOutputStream.get().reset();
    totalUploadedBlockSize += localByte.length;

    CompletableFuture<Void> future = CompletableFuture.runAsync(new Runnable() {
      // call async stageblock and add to future
      @Override
      public void run() {
        byte[] compressedLocalByte = compression.compress(localByte);
        int blockSize = compressedLocalByte.length;

        try {
          ByteBuffer outputStream = ByteBuffer.wrap(compressedLocalByte, 0, blockSize);
          metrics.updateCompressByteMetrics(blockSize);
          LOG.info("{} Upload block start for blob: {} for block size:{}.", blobAsyncClient.getBlobUrl().toString(), blockId, blockSize);
          metrics.updateAzureUploadMetrics();
          // StageBlock generates exception on Failure.
          stageBlock(blockIdEncoded, outputStream, blockSize);
        } catch (Exception e) {
          String msg = String.format("Upload block for blob: %s failed for blockid: %s.", blobAsyncClient.getBlobUrl().toString(), blockId);
          LOG.error(msg, e);
          throw new AzureException(msg, e);
        }
      }
    }, blobThreadPool);

    pendingUpload.add(future);

    future.handle((aVoid, throwable) -> {
      if (throwable == null) {
        LOG.info("Upload block for blob: {} with blockid: {} finished.", blobAsyncClient.getBlobUrl().toString(), blockId);
        pendingUpload.remove(future);
        return aVoid;
      } else {
        throw new AzureException("Blob upload failed for blob " + blobAsyncClient.getBlobUrl().toString()
            + " and block with id: " + blockId, throwable);
      }
    });

    blockNum += 1;
    if (blockNum >= MAX_BLOCKS_IN_AZURE_BLOB) {
      throw new AzureException("Azure blob only supports 50000 blocks in a blob. Current number of blocks is " + blockNum);
    }
  }
}