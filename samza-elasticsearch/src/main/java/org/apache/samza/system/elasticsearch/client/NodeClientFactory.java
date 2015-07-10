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

package org.apache.samza.system.elasticsearch.client;

import org.apache.samza.config.ElasticsearchConfig;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.Map;

/**
 * A {@link ClientFactory} that creates a {@link Node} client that connects to
 * and joins an Elasticsearch cluster.
 *
 * <p>
 * How the Node discovers and joins the cluster can be configured
 * via Elasticsearch settings in the properties of the Samza job.
 * </p>
 */
public class NodeClientFactory implements ClientFactory {
  private final Map<String, String> clientSettings;

  public NodeClientFactory(ElasticsearchConfig config) {
    clientSettings = config.getElasticseachSettings();
  }

  @Override
  public Client getClient() {
    Settings settings = ImmutableSettings.settingsBuilder()
        .put(clientSettings)
        .build();

    Node node = NodeBuilder.nodeBuilder()
        .client(true)
        .settings(settings)
        .build();

    return node.client();
  }
}
