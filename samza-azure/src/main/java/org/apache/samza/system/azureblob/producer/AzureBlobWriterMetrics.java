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

import org.apache.samza.system.azureblob.AzureBlobBasicMetrics;

/**
 * This class holds the basic metrics object of type {@link org.apache.samza.system.azureblob.AzureBlobBasicMetrics}
 * for each group to be tracked.
 * It maintains 3 groups - aggregate metrics, system-level metrics and source metrics.
 *
 * This metrics is used by the AzureBlobWriter to measure the number of bytes written by the writer.
 */
public class AzureBlobWriterMetrics {
  private final AzureBlobBasicMetrics systemMetrics;
  private final AzureBlobBasicMetrics aggregateMetrics;
  private final AzureBlobBasicMetrics sourceMetrics;

  public AzureBlobWriterMetrics(AzureBlobBasicMetrics systemMetrics, AzureBlobBasicMetrics aggregateMetrics,
      AzureBlobBasicMetrics sourceMetrics) {
    this.systemMetrics = systemMetrics;
    this.aggregateMetrics = aggregateMetrics;
    this.sourceMetrics = sourceMetrics;
  }

  /**
   * Increments the write byte metrics counters of all the groups by the number of bytes written.
   * @param dataLength number of bytes written.
   */
  public void updateWriteByteMetrics(long dataLength) {
    systemMetrics.updateWriteByteMetrics(dataLength);
    aggregateMetrics.updateWriteByteMetrics(dataLength);
    sourceMetrics.updateWriteByteMetrics(dataLength);
  }

  /**
   * Increments the compress byte metrics counters of all the groups by the number of compressed bytes written.
   * @param dataLength number of bytes written.
   */
  public void updateCompressByteMetrics(long dataLength) {
    systemMetrics.updateCompressByteMetrics(dataLength);
    aggregateMetrics.updateCompressByteMetrics(dataLength);
    sourceMetrics.updateCompressByteMetrics(dataLength);
  }

  /**
   * Increments the azure block upload metrics counters of all the groups
   */
  public void updateAzureUploadMetrics() {
    systemMetrics.updateAzureUploadMetrics();
    aggregateMetrics.updateAzureUploadMetrics();
    sourceMetrics.updateAzureUploadMetrics();
  }

  /**
   * Increments the azure blob commit metrics counters of all the groups
   */
  public void updateAzureCommitMetrics() {
    systemMetrics.updateAzureCommitMetrics();
    aggregateMetrics.updateAzureCommitMetrics();
    sourceMetrics.updateAzureCommitMetrics();
  }

}
