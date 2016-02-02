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

import org.apache.samza.SamzaException;
import org.elasticsearch.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Elasticsearch configuration class to read elasticsearch specific configuration from Samza.
 */
public class ElasticsearchConfig extends MapConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchConfig.class);

  public static final String CONFIG_KEY_CLIENT_FACTORY = "client.factory";
  public static final String PREFIX_ELASTICSEARCH_SETTINGS = "client.elasticsearch.";

  public static final String CONFIG_KEY_INDEX_REQUEST_FACTORY = "index.request.factory";

  public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
  public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
  public static final String CONFIG_KEY_BULK_FLUSH_INTERVALS_MS = "bulk.flush.interval.ms";

  public static final String CONFIG_KEY_CLIENT_TRANSPORT_HOST = "client.transport.host";
  public static final String CONFIG_KEY_CLIENT_TRANSPORT_PORT = "client.transport.port";

  public ElasticsearchConfig(String name, Config config) {
    super(config.subset("systems." + name + "."));

    logAllSettings(this);
  }

  // Client settings
  public String getClientFactoryClassName() {
    if (containsKey(CONFIG_KEY_CLIENT_FACTORY)) {
      return get(CONFIG_KEY_CLIENT_FACTORY);
    } else {
      throw new SamzaException("You must specify a client factory class"
                               + " for the Elasticsearch system.");
    }
  }

  public Config getElasticseachSettings() {
    return subset(PREFIX_ELASTICSEARCH_SETTINGS);
  }

  // Index Request
  public Optional<String> getIndexRequestFactoryClassName() {
    if (containsKey(CONFIG_KEY_INDEX_REQUEST_FACTORY)) {
      return Optional.of(get(CONFIG_KEY_INDEX_REQUEST_FACTORY));
    } else {
      return Optional.absent();
    }
  }

  // Transport client settings
  public Optional<String> getTransportHost() {
    if (containsKey(CONFIG_KEY_CLIENT_TRANSPORT_HOST)) {
      return Optional.of(get(CONFIG_KEY_CLIENT_TRANSPORT_HOST));
    } else {
      return Optional.absent();
    }
  }

  public Optional<Integer> getTransportPort() {
    if (containsKey(CONFIG_KEY_CLIENT_TRANSPORT_PORT)) {
      return Optional.of(getInt(CONFIG_KEY_CLIENT_TRANSPORT_PORT));
    } else {
      return Optional.absent();
    }
  }

  // Bulk processor settings
  public Optional<Integer> getBulkFlushMaxActions() {
    if (containsKey(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
      return Optional.of(getInt(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS));
    } else {
      return Optional.absent();
    }
  }

  public Optional<Integer> getBulkFlushMaxSizeMB() {
    if (containsKey(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
      return Optional.of(getInt(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB));
    } else {
      return Optional.absent();
    }
  }

  public Optional<Integer> getBulkFlushIntervalMS() {
    if (containsKey(CONFIG_KEY_BULK_FLUSH_INTERVALS_MS)) {
      return Optional.of(getInt(CONFIG_KEY_BULK_FLUSH_INTERVALS_MS));
    } else {
      return Optional.absent();
    }
  }

  private void logAllSettings(Config config) {
    StringBuilder b = new StringBuilder();
    b.append("Elasticsearch System settings: ");
    b.append("\n");
    for (Map.Entry<String, String> entry : config.entrySet()) {
      b.append('\t');
      b.append(entry.getKey());
      b.append(" = ");
      b.append(entry.getValue());
      b.append("\n");
    }
    LOGGER.info(b.toString());
  }
}
