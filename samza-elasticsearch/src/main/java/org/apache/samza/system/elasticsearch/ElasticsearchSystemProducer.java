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

import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.elasticsearch.indexrequest.IndexRequestFactory;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** A {@link SystemProducer} for Elasticsearch that builds on top of the {@link BulkProcessor}
 *
 * <p>
 * Each system that is configured in Samza has an independent {@link BulkProcessor} that flush
 * separably to Elasticsearch. Each {@link BulkProcessor} will maintain the ordering of messages
 * being sent from tasks per Samza container. If you have multiple containers writing to the same
 * message id there is no guarantee of ordering in Elasticsearch.
 * </p>
 *
 * <p>
 * This can be fully configured from the Samza job properties. The client factory and index request
 * are pluggable so the implementation of these can be changed if required.
 * </p>
 *
 * */
public class ElasticsearchSystemProducer implements SystemProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSystemProducer.class);

  private final String system;
  private final Map<String, BulkProcessor> sourceBulkProcessor;
  private final AtomicBoolean sendFailed = new AtomicBoolean(false);
  private final AtomicReference<Throwable> thrown = new AtomicReference<>();

  private final IndexRequestFactory indexRequestFactory;
  private final BulkProcessorFactory bulkProcessorFactory;
  private final ElasticsearchSystemProducerMetrics metrics;

  private Client client;

  public ElasticsearchSystemProducer(String system, BulkProcessorFactory bulkProcessorFactory,
                                     Client client, IndexRequestFactory indexRequestFactory,
                                     ElasticsearchSystemProducerMetrics metrics) {
    this.system = system;
    this.sourceBulkProcessor = new HashMap<>();
    this.bulkProcessorFactory = bulkProcessorFactory;
    this.client = client;
    this.indexRequestFactory = indexRequestFactory;
    this.metrics = metrics;
  }


  @Override
  public void start() {
    // Nothing to do.
  }

  @Override
  public void stop() {
    for (Map.Entry<String, BulkProcessor> e : sourceBulkProcessor.entrySet()) {
      flush(e.getKey());
      e.getValue().close();
    }

    client.close();
  }

  @Override
  public void register(final String source) {
    BulkProcessor.Listener listener = new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
          // Nothing to do.
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
          boolean hasFatalError = false;
          //Do not consider version conficts to be errors. Ignore old versions
          if (response.hasFailures()) {
            for (BulkItemResponse itemResp : response.getItems()) {
              if (itemResp.isFailed()) {
                if (itemResp.getFailure().getStatus().equals(RestStatus.CONFLICT)) {
                  LOGGER.info("Failed to index document in Elasticsearch: " + itemResp.getFailureMessage());
                } else {
                  hasFatalError = true;
                  LOGGER.error("Failed to index document in Elasticsearch: " + itemResp.getFailureMessage());
                }
              }
            }
          }
          if (hasFatalError) {
            sendFailed.set(true);
          } else {
            updateSuccessMetrics(response);
          }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
          LOGGER.error(failure.getMessage());
          thrown.compareAndSet(null, failure);
          sendFailed.set(true);
        }

        private void updateSuccessMetrics(BulkResponse response) {
          metrics.bulkSendSuccess.inc();
          int writes = 0;
          for (BulkItemResponse itemResp: response.getItems()) {
            if (itemResp.isFailed()) {
              if (itemResp.getFailure().getStatus().equals(RestStatus.CONFLICT)) {
                metrics.conflicts.inc();
              }
            } else {
              ActionResponse resp = itemResp.getResponse();
              if (resp instanceof IndexResponse) {
                writes += 1;
                if (((IndexResponse) resp).isCreated()) {
                  metrics.inserts.inc();
                } else {
                  metrics.updates.inc();
                }
              } else {
                LOGGER.error("Unexpected Elasticsearch action response type: " + resp.getClass().getSimpleName());
              }
            }
          }
          LOGGER.info(String.format("Wrote %s messages from %s to %s.",
                  writes, source, system));
        }
    };

    sourceBulkProcessor.put(source, bulkProcessorFactory.getBulkProcessor(client, listener));
  }

  @Override
  public void send(String source, OutgoingMessageEnvelope envelope) {
    IndexRequest indexRequest = indexRequestFactory.getIndexRequest(envelope);
    sourceBulkProcessor.get(source).add(indexRequest);
  }

  @Override
  public void flush(String source) {
    sourceBulkProcessor.get(source).flush();

    if (sendFailed.get()) {
      String message = String.format("Unable to send message from %s to system %s.", source,
                                     system);
      LOGGER.error(message);

      Throwable cause = thrown.get();
      if (cause != null) {
        throw new SamzaException(message, cause);
      } else {
        throw new SamzaException(message);
      }
    }

    LOGGER.info(String.format("Flushed %s to %s.", source, system));
  }

}
