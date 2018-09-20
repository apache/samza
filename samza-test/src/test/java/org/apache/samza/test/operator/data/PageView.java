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
package org.apache.samza.test.operator.data;


import java.io.Serializable;
import org.codehaus.jackson.annotate.JsonProperty;

public class PageView implements Serializable {
  private String viewId;
  private String pageId;
  private String userId;

  public PageView(@JsonProperty("view-id") String viewId,
                  @JsonProperty("page-id") String pageId,
                  @JsonProperty("user-id") String userId) {
    this.viewId = viewId;
    this.pageId = pageId;
    this.userId = userId;
  }

  public String getViewId() {
    return viewId;
  }

  public String getPageId() {
    return pageId;
  }

  public String getUserId() {
    return userId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = viewId != null ? viewId.hashCode() : 0;
    result = prime * result + (pageId != null ? pageId.hashCode() : 0);
    result = prime * result + (userId != null ? userId.hashCode() : 0);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;

    final PageView other = (PageView) obj;

    if (viewId != null
        ? !viewId.equals(other.viewId)
        : other.viewId != null) {
      return false;
    }

    if (pageId != null
        ? !pageId.equals(other.pageId)
        : other.pageId != null) {
      return false;
    }

    return userId != null
        ? userId.equals(other.userId)
        : other.userId == null;
  }

  @Override
  public String toString() {
    return "viewId:" + viewId + "|pageId:" + pageId + "|userId:" + userId;
  }
}
