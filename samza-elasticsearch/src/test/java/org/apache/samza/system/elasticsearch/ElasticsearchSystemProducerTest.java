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
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.elasticsearch.indexrequest.IndexRequestFactory;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticsearchSystemProducerTest {
  private static final String SYSTEM_NAME = "es";
  private static final BulkProcessorFactory BULK_PROCESSOR_FACTORY = mock(BulkProcessorFactory.class);
  private static final Client CLIENT = mock(Client.class);
  private static final IndexRequestFactory INDEX_REQUEST_FACTORY = mock(IndexRequestFactory.class);
  public static final String SOURCE_ONE = "one";
  public static final String SOURCE_TWO = "two";
  private SystemProducer producer;
  public static BulkProcessor processorOne;
  public static BulkProcessor processorTwo;
  private ElasticsearchSystemProducerMetrics metrics;

  @Before
  public void setUp() throws Exception {
    metrics = new ElasticsearchSystemProducerMetrics("es", new MetricsRegistryMap());
    producer = new ElasticsearchSystemProducer(SYSTEM_NAME,
                                               BULK_PROCESSOR_FACTORY,
                                               CLIENT,
                                               INDEX_REQUEST_FACTORY,
                                               metrics);

    processorOne = mock(BulkProcessor.class);
    processorTwo = mock(BulkProcessor.class);

    when(BULK_PROCESSOR_FACTORY.getBulkProcessor(eq(CLIENT), any(BulkProcessor.Listener.class)))
        .thenReturn(processorOne);
    producer.register(SOURCE_ONE);

    when(BULK_PROCESSOR_FACTORY.getBulkProcessor(eq(CLIENT), any(BulkProcessor.Listener.class)))
        .thenReturn(processorTwo);
    producer.register(SOURCE_TWO);
  }

  @Test
  public void testRegisterStop() throws Exception {
    producer.stop();

    verify(processorOne).flush();
    verify(processorTwo).flush();

    verify(processorOne).close();
    verify(processorTwo).close();

    verify(CLIENT).close();
  }

  @Test
  public void testSend() throws Exception {
    OutgoingMessageEnvelope envelope = mock(OutgoingMessageEnvelope.class);
    IndexRequest indexRequest = mock(IndexRequest.class);

    when(INDEX_REQUEST_FACTORY.getIndexRequest(envelope)).thenReturn(indexRequest);

    producer.send(SOURCE_ONE, envelope);

    verify(processorOne).add(indexRequest);
  }

  @Test
  public void testFlushNoFailedSend() throws Exception {
    producer.flush(SOURCE_ONE);

    verify(processorOne).flush();
    verify(processorTwo, never()).flush();
  }

  @Test(expected=SamzaException.class)
  public void testFlushFailedSendFromException() throws Exception {
    ArgumentCaptor<BulkProcessor.Listener> listenerCaptor =
        ArgumentCaptor.forClass(BulkProcessor.Listener.class);

    when(BULK_PROCESSOR_FACTORY.getBulkProcessor(eq(CLIENT), listenerCaptor.capture()))
        .thenReturn(processorOne);
    producer.register(SOURCE_ONE);

    listenerCaptor.getValue().afterBulk(0, null, new Throwable());

    producer.flush(SOURCE_ONE);
  }

  @Test(expected=SamzaException.class)
  public void testFlushFailedSendFromFailedDocument() throws Exception {
    ArgumentCaptor<BulkProcessor.Listener> listenerCaptor =
        ArgumentCaptor.forClass(BulkProcessor.Listener.class);

    when(BULK_PROCESSOR_FACTORY.getBulkProcessor(eq(CLIENT), listenerCaptor.capture()))
        .thenReturn(processorOne);
    producer.register(SOURCE_ONE);

    BulkResponse response = getRespWithFailedDocument(RestStatus.BAD_REQUEST);

    listenerCaptor.getValue().afterBulk(0, null, response);

    producer.flush(SOURCE_ONE);
  }

  @Test
  public void testIgnoreVersionConficts() throws Exception {
    ArgumentCaptor<BulkProcessor.Listener> listenerCaptor =
            ArgumentCaptor.forClass(BulkProcessor.Listener.class);

    when(BULK_PROCESSOR_FACTORY.getBulkProcessor(eq(CLIENT), listenerCaptor.capture()))
            .thenReturn(processorOne);
    producer.register(SOURCE_ONE);

    BulkResponse response = getRespWithFailedDocument(RestStatus.CONFLICT);

    listenerCaptor.getValue().afterBulk(0, null, response);
    assertEquals(1, metrics.conflicts.getCount());

    producer.flush(SOURCE_ONE);
  }

  private BulkResponse getRespWithFailedDocument(RestStatus status) {
    BulkResponse response = mock(BulkResponse.class);
    when(response.hasFailures()).thenReturn(true);

    BulkItemResponse itemResp = mock(BulkItemResponse.class);
    when(itemResp.isFailed()).thenReturn(true);
    BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
    when(failure.getStatus()).thenReturn(status);
    when(itemResp.getFailure()).thenReturn(failure);
    BulkItemResponse[] itemResponses = new BulkItemResponse[]{itemResp};

    when(response.getItems()).thenReturn(itemResponses);

    return response;
  }
}
