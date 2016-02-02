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

package org.apache.samza.config;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ElasticsearchConfigTest {

  private ElasticsearchConfig EMPTY_CONFIG = new ElasticsearchConfig(
      "es",
      new MapConfig(Collections.<String, String>emptyMap()));

  private ElasticsearchConfig configForProperty(String key, String value) {
    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put(key, value);
    return new ElasticsearchConfig("es", new MapConfig(mapConfig));
  }

  @Test
  public void testGetClientFactoryClassName() throws Exception {
    ElasticsearchConfig config = configForProperty("systems.es.client.factory", "bar");

    assertEquals("bar", config.getClientFactoryClassName());
  }

  @Test
  public void testGetTransportHost() throws Exception {
    assertFalse(EMPTY_CONFIG.getTransportHost().isPresent());

    ElasticsearchConfig config = configForProperty("systems.es.client.transport.host", "example.org");

    assertTrue(config.getTransportHost().isPresent());
    assertEquals("example.org", config.getTransportHost().get());
  }

  @Test
  public void testGetTransportPort() throws Exception {
    assertFalse(EMPTY_CONFIG.getTransportPort().isPresent());

    ElasticsearchConfig config = configForProperty("systems.es.client.transport.port", "9300");

    assertTrue(config.getTransportPort().isPresent());
    assertEquals(Integer.valueOf(9300), config.getTransportPort().get());
  }

  @Test
  public void testGetElasticsearchSettings() throws Exception {
    ElasticsearchConfig config = configForProperty("systems.es.client.elasticsearch.foo", "bar");

    assertEquals("bar", config.getElasticseachSettings().get("foo"));
  }

  @Test
  public void testGetBulkFlushMaxActions() throws Exception {
    assertFalse(EMPTY_CONFIG.getBulkFlushMaxActions().isPresent());

    ElasticsearchConfig config = configForProperty("systems.es.bulk.flush.max.actions", "10");

    assertEquals(Integer.valueOf(10), config.getBulkFlushMaxActions().get());
  }

  @Test
  public void testGetBulkFlushMaxSizeMB() throws Exception {
    assertFalse(EMPTY_CONFIG.getBulkFlushMaxSizeMB().isPresent());

    ElasticsearchConfig config = configForProperty("systems.es.bulk.flush.max.size.mb", "10");

    assertTrue(config.getBulkFlushMaxSizeMB().isPresent());
    assertEquals(Integer.valueOf(10), config.getBulkFlushMaxSizeMB().get());
  }

  @Test
  public void testGetBulkFlushIntervalMS() throws Exception {
    assertFalse(EMPTY_CONFIG.getBulkFlushIntervalMS().isPresent());

    ElasticsearchConfig config = configForProperty("systems.es.bulk.flush.interval.ms", "10");

    assertTrue(config.getBulkFlushIntervalMS().isPresent());
    assertEquals(Integer.valueOf(10), config.getBulkFlushIntervalMS().get());
  }

  @Test
  public void testGetIndexRequestFactoryClassName() throws Exception {
    assertFalse(EMPTY_CONFIG.getIndexRequestFactoryClassName().isPresent());

    ElasticsearchConfig config = configForProperty("systems.es.index.request.factory", "foo");

    assertTrue(config.getIndexRequestFactoryClassName().isPresent());
    assertEquals("foo", config.getIndexRequestFactoryClassName().get());
  }
}
