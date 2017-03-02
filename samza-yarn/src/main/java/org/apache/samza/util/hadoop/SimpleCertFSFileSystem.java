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

package org.apache.samza.util.hadoop;

import java.io.IOException;
import java.util.List;
import org.apache.http.NameValuePair;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleCertFSFileSystem extends CertFSFileSystem {
  private static final Logger log = LoggerFactory.getLogger(SimpleCertFSFileSystem.class);

  /**
   * Construct the http request post body payload for CSR request
   * This method can be overriden for customized payload construction
   * @param params
   * @param csr
   * @return
   * @throws IOException
   */
  @Override
  public String constructRequestPayload(final List<NameValuePair> params, final String csr) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode payload = JsonNodeFactory.instance.objectNode();
    for (NameValuePair param: params) {
      log.info("putting param {} in payload", param);
      payload.put(param.getName(), param.getValue());
    }
    payload.put("csr", csr);
    return mapper.writeValueAsString(payload);
  }

  /**
   * Parse the response of the CSR request from CA server
   * This method can be overriden for customized response parsing
   * @param response
   * @return
   * @throws IOException
   */
  @Override
  public String parseResponseForCert(final String response) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode result = mapper.readValue(response, ObjectNode.class);
    return result.get("cert").asText();
  }
}
