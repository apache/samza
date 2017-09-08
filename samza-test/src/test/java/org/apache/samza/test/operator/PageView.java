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
package org.apache.samza.test.operator;

import org.codehaus.jackson.annotate.JsonProperty;

class PageView {
  private String userId;
  private String country;
  private String url;

  /**
   * Constructs a {@link PageView} from the provided string.
   *
   * @param message in the following CSV format - userId,country,url
   */
  PageView(String message) {
    String[] pageViewFields = message.split(",");
    userId = pageViewFields[0];
    country = pageViewFields[1];
    url = pageViewFields[2];
  }

  // the "user-id" property name is required since Samza's JsonSerde's ObjectMapper dasherizes field names.
  public PageView(@JsonProperty("user-id") String userId,
      @JsonProperty("country") String country,
      @JsonProperty("url") String url) {
    this.userId = userId;
    this.country = country;
    this.url = url;
  }

  public String getUserId() {
    return userId;
  }

  public String getCountry() {
    return country;
  }

  public String getUrl() {
    return url;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public void setUrl(String url) {
    this.url = url;
  }
}
