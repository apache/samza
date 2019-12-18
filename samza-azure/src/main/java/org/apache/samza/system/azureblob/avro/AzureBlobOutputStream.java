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
import org.apache.samza.system.azureblob.compression.Compression;
import org.apache.samza.system.azureblob.producer.AzureBlobWriterMetrics;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

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
  private static final int MAX_ATTEMPT = 3;
  private static final int MAX_BLOCKS_IN_AZURE_BLOB = 50000;
  public static final String BLOB_RAW_SIZE_BYTES_METADATA = "rawSizeBytes";
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
  private int blockNum;

  public AzureBlobOutputStream(BlockBlobAsyncClient blobAsyncClient, Executor blobThreadPool, AzureBlobWriterMetrics metrics,
      long flushTimeoutMs, int maxBlockFlushThresholdSize, Compression compression) {
    byteArrayOutputStream = Optional.of(new ByteArrayOutputStream(maxBlockFlushThresholdSize));
    this.blobAsyncClient = blobAsyncClient;
    blockList = new ArrayList<>();
    blockNum = 0;
    this.blobThreadPool = blobThreadPool;
    this.flushTimeoutMs = flushTimeoutMs;
    this.maxBlockFlushThresholdSize = maxBlockFlushThresholdSize;
    this.metrics = metrics;
    this.compression = compression;
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
   * @throws IllegalStateException when
   *       - when closing an already closed stream
   * @throws RuntimeException when
   *       - byteArrayOutputStream.close fails or
   *       - any of the pending uploads fails or
   *       - blob's commitBlockList fails
   */
  @Override
  public synchronized void close() {

    if (isClosed) {
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
      Map<String, String> blobMetadata = Collections.singletonMap(BLOB_RAW_SIZE_BYTES_METADATA, Long.toString(totalUploadedBlockSize));
      blobAsyncClient.commitBlockListWithResponse(blockList, null, blobMetadata, null, null).block();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      String msg = String.format("Close blob %s failed with exception. Total pending sends %d",
          blobAsyncClient.getBlobUrl().toString(), pendingUpload.size());
      throw new RuntimeException(msg, e);
    } catch (Exception e) {
      String msg = String.format("Close blob %s failed with exception. Resetting the system producer. %s",
          blobAsyncClient.getBlobUrl().toString(), e.getLocalizedMessage());
      throw new RuntimeException(msg, e);
    } finally {
      blockList.clear();
      pendingUpload.stream().forEach(future -> future.cancel(true));
      pendingUpload.clear();
      isClosed = true;
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

  @VisibleForTesting
  AzureBlobOutputStream(BlockBlobAsyncClient blobAsyncClient, Executor blobThreadPool, AzureBlobWriterMetrics metrics,
      int flushTimeoutMs, int maxBlockFlushThresholdSize,
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
        int retryCount = 0;
        byte[] compressedLocalByte = compression.compress(localByte);
        int blockSize = compressedLocalByte.length;

        while (retryCount < MAX_ATTEMPT) {
          try {
            ByteBuffer outputStream = ByteBuffer.wrap(compressedLocalByte, 0, blockSize);
            metrics.updateCompressByteMetrics(blockSize);
            LOG.info("{} Upload block start for blob: {} for block size:{}.", blobAsyncClient.getBlobUrl().toString(), blockId, blockSize);
            // StageBlock generates exception on Failure.
            metrics.updateAzureUploadMetrics();
            blobAsyncClient.stageBlock(blockIdEncoded, Flux.just(outputStream), blockSize).block();
            break;
          } catch (Exception e) {
            retryCount += 1;
            String msg = "Upload block for blob: " + blobAsyncClient.getBlobUrl().toString()
                + " failed for blockid: " + blockId + " due to exception. RetryCount: " + retryCount;
            LOG.error(msg, e);
            if (retryCount == MAX_ATTEMPT) {
              throw new RuntimeException("Exceeded number of retries. Max attempts is: " + MAX_ATTEMPT, e);
            }
          }
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
          throw new RuntimeException("Blob upload failed for blob " + blobAsyncClient.getBlobUrl().toString()
              + " and block with id: " + blockId, throwable);
        }
      });

    blockNum += 1;
    if (blockNum >= MAX_BLOCKS_IN_AZURE_BLOB) {
      throw new RuntimeException("Azure blob only supports 50000 blocks in a blob. Current number of blocks is " + blockNum);
    }
  }
}