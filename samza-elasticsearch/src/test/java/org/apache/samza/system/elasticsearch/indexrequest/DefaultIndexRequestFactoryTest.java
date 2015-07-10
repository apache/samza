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

package org.apache.samza.system.elasticsearch.indexrequest;

import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.base.Charsets;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultIndexRequestFactoryTest {

  private static final IndexRequestFactory indexRequestFactory = new DefaultIndexRequestFactory();
  private static final String TYPE = "type";
  private static final String INDEX = "index";
  private static final SystemStream SYSTEM = mock(SystemStream.class);
  private static final Map EMPTY_MSG = Collections.emptyMap();

  @Before
  public void setup() {
    when(SYSTEM.getStream()).thenReturn(INDEX + "/" + TYPE);
  }

  @Test
  public void testGetIndexRequestStreamName()  {
    IndexRequest indexRequest = indexRequestFactory.
        getIndexRequest(new OutgoingMessageEnvelope(SYSTEM, EMPTY_MSG));

    assertEquals(INDEX, indexRequest.index());
    assertEquals(TYPE, indexRequest.type());
  }

  @Test(expected=SamzaException.class)
  public void testGetIndexRequestInvalidStreamName()  {
    when(SYSTEM.getStream()).thenReturn(INDEX);
    indexRequestFactory.getIndexRequest(new OutgoingMessageEnvelope(SYSTEM, EMPTY_MSG));
  }

  @Test
  public void testGetIndexRequestNoId() throws Exception {
    IndexRequest indexRequest =
        indexRequestFactory.getIndexRequest(new OutgoingMessageEnvelope(SYSTEM, EMPTY_MSG));

    assertNull(indexRequest.id());
  }

  @Test
  public void testGetIndexRequestWithId() throws Exception {
    IndexRequest indexRequest =
        indexRequestFactory.getIndexRequest(new OutgoingMessageEnvelope(SYSTEM, "id", EMPTY_MSG));

    assertEquals("id", indexRequest.id());
  }

  @Test
  public void testGetIndexRequestNoPartitionKey() throws Exception {
    IndexRequest indexRequest = indexRequestFactory.getIndexRequest(
        new OutgoingMessageEnvelope(SYSTEM, EMPTY_MSG));

    assertNull(indexRequest.routing());
  }

  @Test
  public void testGetIndexRequestWithPartitionKey() throws Exception {
    IndexRequest indexRequest = indexRequestFactory.getIndexRequest(
        new OutgoingMessageEnvelope(SYSTEM, "shardKey", "id", EMPTY_MSG));

    assertEquals("shardKey", indexRequest.routing());
  }

  @Test
  public void testGetIndexRequestMessageBytes() throws Exception {
    IndexRequest indexRequest = indexRequestFactory.getIndexRequest(
        new OutgoingMessageEnvelope(SYSTEM, "{\"foo\":\"bar\"}".getBytes(Charsets.UTF_8)));

    assertEquals(Collections.singletonMap("foo", "bar"), indexRequest.sourceAsMap());
  }

  @Test
  public void testGetIndexRequestMessageMap() throws Exception {
    IndexRequest indexRequest = indexRequestFactory.getIndexRequest(
        new OutgoingMessageEnvelope(SYSTEM, Collections.singletonMap("foo", "bar")));

    assertEquals(Collections.singletonMap("foo", "bar"), indexRequest.sourceAsMap());
  }

  @Test(expected=SamzaException.class)
  public void testGetIndexRequestInvalidMessage() throws Exception {
    indexRequestFactory.getIndexRequest(new OutgoingMessageEnvelope(SYSTEM, "{'foo':'bar'}"));
  }
}
