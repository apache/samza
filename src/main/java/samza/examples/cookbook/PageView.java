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
package samza.examples.cookbook;

/**
 * Represents a Page view event
 */
class PageView {
  /**
   * The user that viewed the page
   */
  private final String userId;
  /**
   * The region that the page was viewed from
   */
  private final String country;
  /**
   * A trackingId for the page
   */
  private final String pageId;

  /**
   * Constructs a {@link PageView} from the provided string.
   *
   * @param message in the following CSV format - userId,country,url
   */
  PageView(String message) {
    String[] pageViewFields = message.split(",");
    userId = pageViewFields[0];
    country = pageViewFields[1];
    pageId = pageViewFields[2];
  }

  String getUserId() {
    return userId;
  }

  String getCountry() {
    return country;
  }

  String getPageId() {
    return pageId;
  }
}
