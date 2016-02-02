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

import org.apache.samza.config.Config;
import org.apache.samza.config.ElasticsearchConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.elasticsearch.client.ClientFactory;
import org.apache.samza.system.elasticsearch.indexrequest.DefaultIndexRequestFactory;
import org.apache.samza.system.elasticsearch.indexrequest.IndexRequestFactory;
import org.apache.samza.util.Util;
import org.elasticsearch.client.Client;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A {@link SystemFactory} for Elasticsearch.
 *
 * <p>This only supports the {@link SystemProducer} so all other methods return an
 * {@link UnsupportedOperationException}
 */
public class ElasticsearchSystemFactory implements SystemFactory {

  @Override
  public SystemConsumer getConsumer(String name, Config config, MetricsRegistry metricsRegistry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SystemProducer getProducer(String name, Config config, MetricsRegistry metricsRegistry) {
    ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(name, config);
    return new ElasticsearchSystemProducer(name,
                                           getBulkProcessorFactory(elasticsearchConfig),
                                           getClient(elasticsearchConfig),
                                           getIndexRequestFactory(elasticsearchConfig),
                                           new ElasticsearchSystemProducerMetrics(name, metricsRegistry));
  }

  @Override
  public SystemAdmin getAdmin(String name, Config config) {
    return ElasticsearchSystemAdmin.getInstance();
  }


  protected static BulkProcessorFactory getBulkProcessorFactory(ElasticsearchConfig config) {
    return new BulkProcessorFactory(config);
  }

  protected static Client getClient(ElasticsearchConfig config) {
    String name = config.getClientFactoryClassName();
    try {
      Constructor c = Class.forName(name).getConstructor(ElasticsearchConfig.class);

      return ((ClientFactory) c.newInstance(config)).getClient();
    } catch (InvocationTargetException | NoSuchMethodException | InstantiationException
        | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(String.format("Could not instantiate class %s", name));
    }
  }

  protected static IndexRequestFactory getIndexRequestFactory(ElasticsearchConfig config) {
    if (config.getIndexRequestFactoryClassName().isPresent()) {
      return (IndexRequestFactory) Util.getObj(config.getIndexRequestFactoryClassName().get());
    } else {
      return new DefaultIndexRequestFactory();
    }
  }

}
