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

package org.apache.samza.system.elasticsearch;

import org.apache.samza.config.ElasticsearchConfig;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 * Creates Elasticsearch {@link BulkProcessor} instances based on properties from the Samza job.
 */
public class BulkProcessorFactory {
  private final ElasticsearchConfig config;

  public BulkProcessorFactory(ElasticsearchConfig config) {
    this.config = config;
  }

  public BulkProcessor getBulkProcessor(Client client, BulkProcessor.Listener listener) {
    BulkProcessor.Builder builder = BulkProcessor.builder(client, listener);

    // Concurrent requests set to 0 to ensure ordering of documents is maintained in batches.
    // This also means BulkProcessor#flush() is blocking as is also required.
    builder.setConcurrentRequests(0);

    if (config.getBulkFlushMaxActions().isPresent()) {
      builder.setBulkActions(config.getBulkFlushMaxActions().get());
    }
    if (config.getBulkFlushMaxSizeMB().isPresent()) {
      builder.setBulkSize(new ByteSizeValue(config.getBulkFlushMaxSizeMB().get(), ByteSizeUnit.MB));
    }
    if (config.getBulkFlushIntervalMS().isPresent()) {
      builder.setFlushInterval(TimeValue.timeValueMillis(config.getBulkFlushIntervalMS().get()));
    }

    return builder.build();
  }
}
