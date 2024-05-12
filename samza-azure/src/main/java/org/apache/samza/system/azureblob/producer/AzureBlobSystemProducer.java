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

package org.apache.samza.system.azureblob.producer;

import com.azure.core.http.HttpResponse;
import com.azure.core.util.Configuration;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.SkuName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemProducerException;
import org.apache.samza.system.azureblob.AzureBlobClientBuilder;
import org.apache.samza.system.azureblob.AzureBlobConfig;
import org.apache.samza.system.azureblob.BlobClientBuilderFactory;
import org.apache.samza.system.azureblob.compression.CompressionFactory;
import org.apache.samza.system.azureblob.utils.BlobMetadataGeneratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * AzureBlob system producer to send messages to Azure Blob Storage.
 * This system producer is thread safe.
 *     For different sources: sends/flushes can happen in parallel.
 *     For same source: It supports sends in parallel. flushes are exclusive.
 *
 *
 * Azure Blob Storage has a 3 level hierarchy: an Azure account contains multiple containers (akin to directories
 * in a file system) and each container has multiple blobs (akin to files).
 *
 * Azure Container: System name maps to the name of Azure container.
 * An instance of a system producer writes to a single Azure container considering the container as a system.
 *
 * Azure Blob: For a given stream-partition pair, a blob is created with name stream/partition/timestamp-randomString.
 * The stream and partition are extracted from the SSP of OutgoingMessageEnvelope in send().
 * Blob is started when the first message for that stream-partition is sent by a source
 * and closed during flush for that source.
 * Subsequent sends by the source to the same stream-partition will create a new blob with a different timestamp.
 * Thus, timestamp corresponds to writer creation time i.e; the first send for source-SSP
 * or first send after a flush for the source.
 * If max blob size or record limit are configured, then a new blob is started when limits exceed.
 *
 * A random string is used as a suffix in the blob name to prevent collisions:
 *  - if two system producers are writing to the same SSP.
 *  - if two sources send to the same SSP.
 *
 * Lifecycle of the system producer is shown below. All sources have to be registered before starting the producer.
 * Several messages can be sent by a source via send(source, envelope). This can be followed by a flush(source) or stop()
 * After flush(source), more messages can be sent for that source and  other sources as well. stop() internally calls
 * flush(source) for all the sources registered. After stop(), no calls to send and flush are allowed.
 *
 *
 *                                                             ┌──────────────────────────────┐
 *                                                             │                              │
 *                                                             ▼                              │
 * Lifecycle: register(source) ────────▶ start() ──────▶ send(source, envelope) ──────▶ flush(source) ──────▶ stop()
 *            [multiple times                             │    ▲          │                                     ▲
 *                   for                                  └────┘          └─────────────────────────────────────┘
 *            multiple sources]
 *
 * This SystemProducer does not open up the envelopes sent through it. It is the responsibility of the user of this
 * SystemProducer to ensure the envelopes are valid and a correct writer has been chosen by wiring up the
 * writer factory config.
 *
 */
public class AzureBlobSystemProducer implements SystemProducer {

  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobSystemProducer.class.getName());

  private static final String BLOB_NAME_PREFIX = "%s";
  private static final String BLOB_NAME_PARTITION_PREFIX = "%s/%s";

  private static final String AZURE_URL = "https://%s.blob.core.windows.net";

  private static final int PREMIUM_MAX_BLOCK_SIZE = 100 * 1024 * 1024; // 100MB
  private static final int STANDARD_MAX_BLOCK_SIZE = 4 * 1024 * 1024; // 4MB

  private BlobContainerAsyncClient containerAsyncClient;
  private final String systemName;
  private final AzureBlobConfig config;

  // Map of writers indexed first by sourceName and then by (streamName, partitionName) or just streamName if partition key does not exist.
  private final Map<String, Map<String, AzureBlobWriter>> writerMap;
  private final AzureBlobWriterFactory writerFactory;
  private final BlobClientBuilderFactory clientFactory;

  private final int blockFlushThresholdSize;
  private final long flushTimeoutMs;
  private final long closeTimeout;
  private final ThreadPoolExecutor asyncBlobThreadPool;

  private volatile boolean isStarted = false;
  private volatile boolean isStopped = false;

  private final AzureBlobSystemProducerMetrics metrics;

  private final Map<String, Object> sourceWriterCreationLockMap = new ConcurrentHashMap<>();
  private final Map<String, ReadWriteLock> sourceSendFlushLockMap = new ConcurrentHashMap<>();

  private final BlobMetadataGeneratorFactory blobMetadataGeneratorFactory;
  private final Config blobMetadataGeneratorConfig;

  public AzureBlobSystemProducer(String systemName, AzureBlobConfig config, MetricsRegistry metricsRegistry) {
    Preconditions.checkNotNull(systemName, "System name can not be null when creating AzureBlobSystemProducer");
    Preconditions.checkNotNull(config, "Config can not be null when creating AzureBlobSystemProducer");
    Preconditions.checkNotNull(metricsRegistry, "Metrics registry can not be null when creating AzureBlobSystemProducer");

    // Azure logs do not show without this property set
    System.setProperty(Configuration.PROPERTY_AZURE_LOG_LEVEL, "1");
    this.systemName = systemName;
    this.config = config;

    String clientFactoryClassName = this.config.getAzureBlobClientBuilderFactoryClassName(this.systemName);
    try {
      this.clientFactory = (BlobClientBuilderFactory) Class.forName(clientFactoryClassName).newInstance();
    } catch (Exception e) {
      throw new SystemProducerException("Could not create blob client factory with name " + clientFactoryClassName, e);
    }

    String writerFactoryClassName = this.config.getAzureBlobWriterFactoryClassName(this.systemName);
    try {
      this.writerFactory = (AzureBlobWriterFactory) Class.forName(writerFactoryClassName).newInstance();
    } catch (Exception e) {
      throw new SystemProducerException("Could not create writer factory with name " + writerFactoryClassName, e);
    }
    this.flushTimeoutMs = this.config.getFlushTimeoutMs(this.systemName);
    this.closeTimeout = this.config.getCloseTimeoutMs(this.systemName);
    this.blockFlushThresholdSize = this.config.getMaxFlushThresholdSize(this.systemName);
    int asyncBlobThreadPoolCount = this.config.getAzureBlobThreadPoolCount(this.systemName);
    int blockingQueueSize = this.config.getBlockingQueueSize(this.systemName);

    LOG.info("SystemName: {} block flush size:{}", systemName, this.blockFlushThresholdSize);
    LOG.info("SystemName: {} thread count:{}", systemName, asyncBlobThreadPoolCount);

    BlockingQueue<Runnable>
        linkedBlockingDeque = new LinkedBlockingDeque<>(blockingQueueSize);

    this.asyncBlobThreadPool =
        new ThreadPoolExecutor(asyncBlobThreadPoolCount, asyncBlobThreadPoolCount, 60,
            TimeUnit.SECONDS, linkedBlockingDeque, new ThreadPoolExecutor.CallerRunsPolicy());

    this.writerMap = new ConcurrentHashMap<>();

    this.metrics = new AzureBlobSystemProducerMetrics(systemName, config.getAzureAccountName(systemName), metricsRegistry);

    String blobMetadataGeneratorFactoryClassName = this.config.getSystemBlobMetadataPropertiesGeneratorFactory(this.systemName);
    try {
      blobMetadataGeneratorFactory = (BlobMetadataGeneratorFactory) Class.forName(blobMetadataGeneratorFactoryClassName).newInstance();
    } catch (Exception e) {
      throw new SystemProducerException("Could not create blob metadata generator factory with name " + blobMetadataGeneratorFactoryClassName, e);
    }
    blobMetadataGeneratorConfig = this.config.getSystemBlobMetadataGeneratorConfigs(systemName);
  }

  /**
   * {@inheritDoc}
   * @throws SystemProducerException
   */
  @Override
  public synchronized void start() {
    if (isStarted) {
      LOG.warn("Attempting to start an already started producer.");
      return;
    }
    setupAzureContainer();

    LOG.info("Starting producer.");
    isStarted = true;
  }

  /**
   * {@inheritDoc}
   * @throws SystemProducerException
   */
  @Override
  public synchronized void stop() {
    if (!isStarted) {
      LOG.warn("Attempting to stop a producer that was not started.");
      return;
    }

    if (isStopped) {
      LOG.warn("Attempting to stop an already stopped producer.");
      return;
    }

    try {
      writerMap.forEach((source, sourceWriterMap) -> flush(source));
      asyncBlobThreadPool.shutdown();
      isStarted = false;
    } catch (Exception e) {
      throw new SystemProducerException("Stop failed with exception.", e);
    } finally {
      writerMap.clear();
      isStopped = true;
    }
  }

  /**
   * {@inheritDoc}
   * @throws SystemProducerException
   */
  @Override
  public synchronized void register(String source) {
    LOG.info("Registering source {}", source);
    if (isStarted) {
      throw new SystemProducerException("Cannot register once the producer is started.");
    }
    if (writerMap.containsKey(source)) {
      // source already registered => writerMap and metrics have entries for the source
      LOG.warn("Source: {} already registered", source);
      return;
    }
    writerMap.put(source, new ConcurrentHashMap<>());
    sourceWriterCreationLockMap.put(source, new Object());
    sourceSendFlushLockMap.put(source, new ReentrantReadWriteLock());
    metrics.register(source);
  }

  /**
   * Multi-threading and thread-safety:
   *
   *  From Samza usage of SystemProducer:
   *  The lifecycle of SystemProducer shown above is consistent with most use cases within Samza (with the exception of
   *  Coordinator stream store/producer and KafkaCheckpointManager).
   *  A single parent thread creates the SystemProducer, registers all sources and starts it before handing it
   *  to multiple threads for use (send and flush). Finally, the single parent thread stops the producer.
   *  The most frequent operations on a SystemProducer are send and flush while register, start and stop are one-time operations.
   *
   *  Based on this usage pattern: to provide multi-threaded support and improve throughput of this SystemProducer,
   *  multiple sends and flushes need to happen in parallel. However, the following rules are needed to ensure
   *  o data loss and data consistency.
   *  1. sends can happen in parallel for same source or different sources.
   *  2. send and flush for the same source can not happen in parallel. Although, the AzureBlobWriter is thread safe,
   *     interleaving write and flush and close operations of a writer can lead to data loss if a write happens between flush and close.
   *     There are other scenarios such as issuing a write to the writer after close and so on.
   *  3. writer creation for the same writer key (SSP) can not happen in parallel - for the reason that multiple
   *     writers could get created with only one being retained but all being used and GCed after a send, leading to data loss.
   *
   *  These 3 rules are achieved by using a per source ReadWriteLock to allow sends in parallel but guarantee exclusivity for flush.
   *  Additionally, a per source lock is used to ensure writer creation is in a critical section.
   *
   *  Concurrent access to shared objects as follows:
   *  1. AzureBlobWriters is permitted as long as there are no interleaving of operations for a writer.
   *     If multiple operations of writer (as in flush) then make it synchronized.
   *  2. ConcurrentHashMaps (esp writerMap per source) get and put - disallow interleaving by doing put and clear under locks.
   *  3. WriterFactory and Metrics are thread-safe. WriterFactory is stateless while Metrics' operations interleaving
   *     is thread-safe too as they work on different counters.
   *  The above locking mechanisms ensure thread-safety.
   * {@inheritDoc}
   * @throws SystemProducerException
   */
  @Override
  public void send(String source, OutgoingMessageEnvelope messageEnvelope) {
    if (!isStarted) {
      throw new SystemProducerException("Trying to send before producer has started.");
    }

    if (isStopped) {
      throw new SystemProducerException("Sending after producer has been stopped.");
    }

    ReadWriteLock lock = sourceSendFlushLockMap.get(source);
    if (lock == null) {
      throw new SystemProducerException("Attempting to send to source: " + source + " but it was not registered");
    }
    lock.readLock().lock();
    try {
      AzureBlobWriter writer = getOrCreateWriter(source, messageEnvelope);
      writer.write(messageEnvelope);
      metrics.updateWriteMetrics(source);
    } catch (Exception e) {
      metrics.updateErrorMetrics(source);
      Object partitionKey = getPartitionKey(messageEnvelope);
      String msg = "Send failed for source: " + source + ", system: " + systemName
          + ", stream: " + messageEnvelope.getSystemStream().getStream()
          + ", partitionKey: " + ((partitionKey != null) ? partitionKey : "null");
      throw new SystemProducerException(msg, e);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * {@inheritDoc}
   * @throws SystemProducerException
   */
  @Override
  public void flush(String source) {
    if (!isStarted) {
      throw new SystemProducerException("Trying to flush before producer has started.");
    }

    if (isStopped) {
      throw new SystemProducerException("Flushing after producer has been stopped.");
    }

    ReadWriteLock lock = sourceSendFlushLockMap.get(source);
    if (lock == null) {
      throw new SystemProducerException("Attempting to flush source: " + source + " but it was not registered");
    }
    lock.writeLock().lock();
    Map<String, AzureBlobWriter> sourceWriterMap = writerMap.get(source);
    try {
      // first flush all the writers
      // then close and remove all the writers
      flushWriters(sourceWriterMap);
      closeWriters(source, sourceWriterMap);
    } catch (Exception e) {
      metrics.updateErrorMetrics(source);
      throw new SystemProducerException("Flush failed for system:" + systemName + " and source: " + source, e);
    } finally {
      sourceWriterMap.clear();
      lock.writeLock().unlock();
    }
  }

  @VisibleForTesting
  void setupAzureContainer() {
    try {
      BlobServiceAsyncClient storageClient =
          clientFactory.getBlobClientBuilder(systemName, AZURE_URL, config).getBlobServiceAsyncClient();
      validateFlushThresholdSizeSupported(storageClient);

      containerAsyncClient = storageClient.getBlobContainerAsyncClient(systemName);

      // Only way to check if container exists or not is by creating it and look for failure/success.
      createContainerIfNotExists(containerAsyncClient);
    } catch (Exception e) {
      metrics.updateAzureContainerMetrics();
      throw new SystemProducerException("Failed to set up Azure container for SystemName: " + systemName, e);
    }
  }

  void validateFlushThresholdSizeSupported(BlobServiceAsyncClient storageClient) {
    long flushThresholdSize = config.getMaxFlushThresholdSize(systemName);
    try {
      SkuName accountType = storageClient.getAccountInfo().block().getSkuName();
      String accountName = storageClient.getAccountName();
      boolean isPremiumAccount = SkuName.PREMIUM_LRS == accountType;
      if (isPremiumAccount && flushThresholdSize > PREMIUM_MAX_BLOCK_SIZE) { // 100 MB
        throw new SystemProducerException("Azure storage account with name: " + accountName + " is a premium account and can only handle upto "
            + PREMIUM_MAX_BLOCK_SIZE + " threshold size. Given flush threshold size is " + flushThresholdSize);
      } else if (!isPremiumAccount && flushThresholdSize > STANDARD_MAX_BLOCK_SIZE) { // STANDARD account
        throw new SystemProducerException(
            "Azure storage account with name: " + accountName + " is a standard account and can only handle upto " + STANDARD_MAX_BLOCK_SIZE + " threshold size. Given flush threshold size is "
                + flushThresholdSize);
      }
    } catch (Exception e) {
      LOG.warn("Exception encountered while trying to ensure that the given flush threshold size is "
              + "supported by the desired azure blob storage account. "
              + "{} is the given account name and {} is the flush threshold size.",
          config.getAzureAccountName(systemName), flushThresholdSize);
      LOG.warn("SystemProducer will continue and send messages to Azure Blob Storage but they might fail "
          + "if the given threshold size is not supported by the account");
    }
  }

  /**
   * // find the writer in the writerMap else create one
   * @param source for which to find/create the writer
   * @param messageEnvelope to fetch the schema from if writer needs to be created
   * @return an AzureBlobWriter object
   */
  @VisibleForTesting
  AzureBlobWriter getOrCreateWriter(String source, OutgoingMessageEnvelope messageEnvelope) {
    String writerMapKey;
    String blobURLPrefix;
    String partitionKey = getPartitionKey(messageEnvelope);
    // using most significant bits in UUID (8 digits) to avoid collision in blob names
    if (partitionKey == null) {
      writerMapKey = messageEnvelope.getSystemStream().getStream();
      blobURLPrefix = String.format(BLOB_NAME_PREFIX, messageEnvelope.getSystemStream().getStream());
    } else {
      writerMapKey = messageEnvelope.getSystemStream().getStream() + "/" + partitionKey;
      blobURLPrefix = String.format(BLOB_NAME_PARTITION_PREFIX, messageEnvelope.getSystemStream().getStream(), partitionKey);
    }
    Map<String, AzureBlobWriter> sourceWriterMap = writerMap.get(source);
    if (sourceWriterMap == null) {
      throw new SystemProducerException("Attempting to send to source: " + source + " but it is not registered");
    }
    AzureBlobWriter writer = sourceWriterMap.get(writerMapKey);
    if (writer == null) {
      synchronized (sourceWriterCreationLockMap.get(source)) {
        writer = sourceWriterMap.get(writerMapKey);
        if (writer == null) {
          AzureBlobWriterMetrics writerMetrics =
              new AzureBlobWriterMetrics(metrics.getAggregateMetrics(), metrics.getSystemMetrics(), metrics.getSourceMetrics(source));
          writer = createNewWriter(blobURLPrefix, writerMetrics, messageEnvelope.getSystemStream().getStream());
          sourceWriterMap.put(writerMapKey, writer);
        }
      }
    }
    return writer;
  }

  private void createContainerIfNotExists(BlobContainerAsyncClient containerClient) {
    try {
      containerClient.create().block();
    } catch (BlobStorageException e) {
      //StorageErrorCode defines constants corresponding to all error codes returned by the service.
      if (e.getErrorCode() == BlobErrorCode.RESOURCE_NOT_FOUND) {
        HttpResponse response = e.getResponse();
        LOG.error("Error creating the container url " + containerClient.getBlobContainerUrl().toString() + " with status code: " + response.getStatusCode(), e);
      } else if (e.getErrorCode() == BlobErrorCode.CONTAINER_BEING_DELETED) {
        LOG.error("Container is being deleted. Container URL is: " + containerClient.getBlobContainerUrl().toString(), e);
      } else if (e.getErrorCode() == BlobErrorCode.CONTAINER_ALREADY_EXISTS) {
        return;
      }
      throw e;
    }
  }

  private String getPartitionKey(OutgoingMessageEnvelope messageEnvelope) {
    Object partitionKey = messageEnvelope.getPartitionKey();
    if (partitionKey == null || !(partitionKey instanceof String)) {
      return null;
    }
    return (String) partitionKey;
  }

  private void flushWriters(Map<String, AzureBlobWriter> sourceWriterMap) {
    sourceWriterMap.forEach((stream, writer) -> {
      try {
        LOG.info("Flushing topic:{}", stream);
        writer.flush();
      } catch (IOException e) {
        throw new SystemProducerException("Close failed for topic " + stream, e);
      }
    });
  }

  private void closeWriters(String source, Map<String, AzureBlobWriter> sourceWriterMap) throws Exception {
    Set<CompletableFuture<Void>> pendingClose = ConcurrentHashMap.newKeySet();
    try {
      sourceWriterMap.forEach((stream, writer) -> {
        LOG.info("Closing topic:{}", stream);
        CompletableFuture<Void> future = CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
              try {
                writer.close();
              } catch (Exception e) {
                throw new SystemProducerException("Close failed for topic " + stream, e);
              }
            }
          }, asyncBlobThreadPool);
        pendingClose.add(future);
        future.handle((aVoid, throwable) -> {
          sourceWriterMap.remove(writer);
          if (throwable != null) {
            throw new SystemProducerException("Close failed for topic " + stream, throwable);
          } else {
            LOG.info("Blob close finished for stream " + stream);
            return aVoid;
          }
        });
      });
      CompletableFuture<Void> future = CompletableFuture.allOf(pendingClose.toArray(new CompletableFuture[0]));
      LOG.info("Flush source: {} has pending closes: {} ", source, pendingClose.size());
      future.get((long) closeTimeout, TimeUnit.MILLISECONDS);
    } finally {
      pendingClose.clear();
    }
  }

  @VisibleForTesting
  AzureBlobWriter createNewWriter(String blobURL, AzureBlobWriterMetrics writerMetrics, String streamName) {
    try {
      return writerFactory.getWriterInstance(containerAsyncClient, blobURL, asyncBlobThreadPool, writerMetrics,
          blobMetadataGeneratorFactory, blobMetadataGeneratorConfig, streamName,
          blockFlushThresholdSize, flushTimeoutMs,
          CompressionFactory.getInstance().getCompression(config.getCompressionType(systemName)),
          config.getSuffixRandomStringToBlobName(systemName),
          config.getMaxBlobSize(systemName),
          config.getMaxMessagesPerBlob(systemName),
          config.getInitBufferSizeBytes(systemName));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create a writer for the producer.", e);
    }
  }

}