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
package samza.examples.cookbook.data;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * A page view event
 */
public class PageView {
  public final String userId;
  public final String country;
  public final String pageId;

  /**
   * Constructs a page view event.
   *
   * @param pageId the id for the page that was viewed
   * @param userId the user that viewed the page
   * @param country the country that the page was viewed from
   */
  public PageView(
      @JsonProperty("pageId") String pageId,
      @JsonProperty("userId") String userId,
      @JsonProperty("countryId") String country) {
    this.userId = userId;
    this.country = country;
    this.pageId = pageId;
  }
}
