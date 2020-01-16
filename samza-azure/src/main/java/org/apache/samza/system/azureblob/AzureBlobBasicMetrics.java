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

package org.apache.samza.system.azureblob;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;

/**
 * This class holds all the metrics to be measured (like write, write byte, error)
 * for a single group (like aggregate, system or source).
 */
public class AzureBlobBasicMetrics {
  public static final String EVENT_WRITE_RATE = "eventWriteRate";
  public static final String EVENT_PRODUCE_ERROR = "eventProduceError";
  public static final String EVENT_WRITE_BYTE_RATE = "eventWriteByteRate";
  public static final String EVENT_COMPRESS_BYTE_RATE = "eventCompressByteRate";
  public static final String AZURE_BLOCK_UPLOAD_RATE = "azureBlockUploadRate";
  public static final String AZURE_BLOB_COMMIT_RATE = "azureBlobCommitRate";

  private final Counter writeMetrics;
  private final Counter writeByteMetrics;
  private final Counter errorMetrics;
  private final Counter compressByteMetrics;
  private final Counter azureUploadMetrics;
  private final Counter azureCommitMetrics;

  public AzureBlobBasicMetrics(String group, MetricsRegistry metricsRegistry) {
    writeMetrics = metricsRegistry.newCounter(group, EVENT_WRITE_RATE);
    errorMetrics = metricsRegistry.newCounter(group, EVENT_PRODUCE_ERROR);
    writeByteMetrics = metricsRegistry.newCounter(group, EVENT_WRITE_BYTE_RATE);
    compressByteMetrics = metricsRegistry.newCounter(group, EVENT_COMPRESS_BYTE_RATE);
    azureUploadMetrics = metricsRegistry.newCounter(group, AZURE_BLOCK_UPLOAD_RATE);
    azureCommitMetrics = metricsRegistry.newCounter(group, AZURE_BLOB_COMMIT_RATE);
  }

  /**
   * Increments the write metrics counter by 1.
   */
  public void updateWriteMetrics() {
    writeMetrics.inc();
  }

  /**
   * Increments the write byte metrics counter by the number of bytes written.
   * @param dataLength number of bytes written.
   */
  public void updateWriteByteMetrics(long dataLength) {
    writeByteMetrics.inc(dataLength);
  }

  /**
   * Increments the compress byte metrics counter by the number of compressed bytes written.
   * @param dataLength number of bytes written.
   */
  public void updateCompressByteMetrics(long dataLength) {
    compressByteMetrics.inc(dataLength);
  }

  /**
   * Increments the error metrics counter by 1.
   */
  public void updateErrorMetrics() {
    errorMetrics.inc();
  }


  /**
   * Increments the azure block upload metrics counter by 1.
   */
  public void updateAzureUploadMetrics() {
    azureUploadMetrics.inc();
  }


  /**
   * Increments the azure blob commit metrics counter by 1.
   */
  public void updateAzureCommitMetrics() {
    azureCommitMetrics.inc();
  }
}
