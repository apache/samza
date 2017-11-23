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
package org.apache.samza.webapp;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * Client for the {@link ApplicationMasterRestServlet}.
 */
public class ApplicationMasterRestClient implements Closeable {
  private final CloseableHttpClient httpClient;
  private final HttpHost appMasterHost;
  private final ObjectMapper jsonMapper = SamzaObjectMapper.getObjectMapper();

  public ApplicationMasterRestClient(CloseableHttpClient client, String amHostName, int amRpcPort) {
    httpClient = client;
    appMasterHost = new HttpHost(amHostName, amRpcPort);
  }

  /**
   * @return  the metrics as a map of groupName to metricName to metricValue.
   * @throws IOException if there was an error fetching the metrics from the servlet.
   */
  public Map<String, Map<String, Object>> getMetrics() throws IOException {
    String jsonString = getEntityAsJson("/metrics", "metrics");
    return jsonMapper.readValue(jsonString, new TypeReference<Map<String, Map<String, Object>>>() {});
  }

  /**
   * @return  the task context as a map of key to value
   * @throws IOException if there was an error fetching the task context from the servlet.
   */
  public Map<String, Object> getTaskContext() throws IOException {
    String jsonString = getEntityAsJson("/task-context", "task context");
    return jsonMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
  }

  /**
   * @return  the AM state as a map of key to value
   * @throws IOException if there was an error fetching the AM state from the servlet.
   */
  public Map<String, Object> getAmState() throws IOException {
    String jsonString = getEntityAsJson("/am", "AM state");
    return jsonMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
  }

  /**
   * @return  the config as a map of key to value
   * @throws IOException if there was an error fetching the config from the servlet.
   */
  public Map<String, Object> getConfig() throws IOException {
    String jsonString = getEntityAsJson("/config", "config");
    return jsonMapper.readValue(jsonString, new TypeReference<Map<String, Object>>() {});
  }

  @Override
  public void close() throws IOException {
    httpClient.close();
  }

  private String getEntityAsJson(String path, String entityName) throws IOException {
    HttpGet getRequest = new HttpGet(path);
    HttpResponse httpResponse = httpClient.execute(appMasterHost, getRequest);

    StatusLine status = httpResponse.getStatusLine();
    if (status.getStatusCode() != HttpStatus.SC_OK) {
      throw new SamzaException(String.format(
          "Error retrieving %s from host %s. Response: %s",
          entityName,
          appMasterHost.toURI(),
          status.getReasonPhrase()));
    }

    return EntityUtils.toString(httpResponse.getEntity());
  }
}
