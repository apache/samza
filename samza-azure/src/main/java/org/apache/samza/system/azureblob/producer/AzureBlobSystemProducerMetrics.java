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
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class holds all the metrics for a {@link org.apache.samza.system.azureblob.producer.AzureBlobSystemProducer}.
 * It maintains: aggregate metrics, system level metrics and source metrics for all the sources that register and
 * send through the SystemProducer. It has a map for holding the metrics of all sources.
 *
 * Apart from the basic metrics for each group, this class also holds metrics for Azure container creation errors.
 */
public class AzureBlobSystemProducerMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobSystemProducerMetrics.class.getName());

  protected static final String AZURE_CONTAINER_ERROR = "azureContainerError";
  protected static final String AGGREGATE = "aggregate";
  protected static final String SYSTEM_METRIC_FORMAT = "%s_%s";

  private final MetricsRegistry metricsRegistry;
  private final Map<String, AzureBlobBasicMetrics> sourceMetricsMap;
  private final AzureBlobBasicMetrics aggregateMetrics;
  private final AzureBlobBasicMetrics systemMetrics;
  private final Counter aggregateAzureContainerErrorMetrics;
  private final Counter systemAzureContainerErrorMetrics;

  private final String systemName;
  private final String accountName;

  public AzureBlobSystemProducerMetrics(String systemName, String accountName, MetricsRegistry metricsRegistry) {
    this.metricsRegistry = metricsRegistry;
    this.systemName = systemName;
    this.accountName = accountName;

    sourceMetricsMap = new HashMap<>();
    aggregateMetrics = new AzureBlobBasicMetrics(AGGREGATE, metricsRegistry);
    systemMetrics = new AzureBlobBasicMetrics(String.format(SYSTEM_METRIC_FORMAT, accountName, systemName), metricsRegistry);
    aggregateAzureContainerErrorMetrics = metricsRegistry.newCounter(AGGREGATE, AZURE_CONTAINER_ERROR);
    systemAzureContainerErrorMetrics = metricsRegistry.newCounter(String.format(SYSTEM_METRIC_FORMAT, accountName, systemName), AZURE_CONTAINER_ERROR);
  }

  /**
   * Adds a AzureBlobBasicMetrics object for the source being registered with the SystemProducer.
   * @param source to be registered.
   */
  public void register(String source) {
    if (systemName.equals(source)) {
      // source is the same as the system name. creating counters for source name will double count metrics
      LOG.warn("Source:{} is the same as the system name.", source);
      return;
    }
    sourceMetricsMap.putIfAbsent(source, new AzureBlobBasicMetrics(source, metricsRegistry));
  }

  /**
   * Increments the error metrics counters of aggregate, system and the source by 1.
   * @param source for which the error occurred.
   */
  public void updateErrorMetrics(String source) {
    AzureBlobBasicMetrics sourceMetrics = sourceMetricsMap.get(source);
    if (sourceMetrics != null) {
      sourceMetrics.updateErrorMetrics();
    }
    incrementErrorMetrics();
  }

  /**
   * Increments the write metrics counters of aggregate, system and the source by 1.
   * Write metrics is for number of messages successfully written to the source.
   * @param source for which the message was sent.
   */
  public void updateWriteMetrics(String source) {
    AzureBlobBasicMetrics sourceMetrics = sourceMetricsMap.get(source);
    if (sourceMetrics != null) {
      sourceMetrics.updateWriteMetrics();
    }
    incrementWriteMetrics();
  }

  /**
   * Increments the Azure container creation error metrics by 1.
   */
  public void updateAzureContainerMetrics() {
    aggregateAzureContainerErrorMetrics.inc();
    systemAzureContainerErrorMetrics.inc();
  }

  public AzureBlobBasicMetrics getAggregateMetrics() {
    return aggregateMetrics;
  }

  public AzureBlobBasicMetrics getSystemMetrics() {
    return systemMetrics;
  }

  public AzureBlobBasicMetrics getSourceMetrics(String source) {
    return sourceMetricsMap.get(source);
  }

  private void incrementWriteMetrics() {
    aggregateMetrics.updateWriteMetrics();
    systemMetrics.updateWriteMetrics();
  }

  private void incrementErrorMetrics() {
    aggregateMetrics.updateErrorMetrics();
    systemMetrics.updateErrorMetrics();
  }
}
