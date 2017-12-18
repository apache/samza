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

package org.apache.samza.test.controlmessages;

import java.io.Serializable;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class TestData {

  public static class PageView implements Serializable {
    @JsonProperty("pageKey")
    final String pageKey;
    @JsonProperty("memberId")
    final int memberId;

    @JsonProperty("pageKey")
    public String getPageKey() {
      return pageKey;
    }

    @JsonProperty("memberId")
    public int getMemberId() {
      return memberId;
    }

    @JsonCreator
    public PageView(@JsonProperty("pageKey") String pageKey, @JsonProperty("memberId") int memberId) {
      this.pageKey = pageKey;
      this.memberId = memberId;
    }
  }

  public static class PageViewJsonSerdeFactory implements SerdeFactory<PageView> {
    @Override
    public Serde<PageView> getSerde(String name, Config config) {
      return new PageViewJsonSerde();
    }
  }

  public static class PageViewJsonSerde implements Serde<PageView> {
    ObjectMapper mapper = new ObjectMapper();

    @Override
    public PageView fromBytes(byte[] bytes) {
      try {
        return mapper.readValue(new String(bytes, "UTF-8"), new TypeReference<PageView>() { });
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }

    @Override
    public byte[] toBytes(PageView pv) {
      try {
        return mapper.writeValueAsString(pv).getBytes("UTF-8");
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    }
  }
}
