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

package org.apache.samza.serializers;

import org.apache.samza.SamzaException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * A serializer for UTF-8 encoded JSON strings. JsonSerdeV2 differs from JsonSerde in that:
 * <ol>
 *   <li>
 *     It allows specifying the specific POJO type to deserialize to (using JsonSerdeV2(Class&lt;T&gt;)
 *     or JsonSerdeV2#of(Class&lt;T&gt;). JsonSerde always returns a LinkedHashMap&lt;String, Object&gt;
 *     upon deserialization.
 *   <li>
 *     It uses Jackson's default 'camelCase' property naming convention, which simplifies defining
 *     the POJO to bind to. JsonSerde enforces the 'dash-separated' property naming convention.
 * </ol>
 * This JsonSerdeV2 should be preferred over JsonSerde for High Level API applications, unless
 * backwards compatibility with the older data format (with dasherized names) is required.
 *
 * @param <T> the type of the POJO being (de)serialized.
 */
public class JsonSerdeV2<T> implements Serde<T> {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSerdeV2.class);
  private final Class<T> clazz;
  private transient final ObjectMapper mapper = new ObjectMapper();

  /**
   * Constructs a JsonSerdeV2 that returns a LinkedHashMap&lt;String, Object&lt; upon deserialization.
   */
  public JsonSerdeV2() {
    this(null);
  }

  /**
   * Constructs a JsonSerdeV2 that (de)serializes POJOs of class {@code clazz}.
   *
   * @param clazz the class of the POJO being (de)serialized.
   */
  public JsonSerdeV2(Class<T> clazz) {
    this.clazz = clazz;
  }

  public static <T> JsonSerdeV2<T> of(Class<T> clazz) {
    return new JsonSerdeV2<>(clazz);
  }

  public byte[] toBytes(T obj) {
    if (obj != null) {
      try {
        String str = mapper.writeValueAsString(obj);
        return str.getBytes("UTF-8");
      } catch (Exception e) {
        throw new SamzaException("Error serializing data.", e);
      }
    } else {
      return null;
    }
  }

  public T fromBytes(byte[] bytes) {
    if (bytes != null) {
      String str;
      try {
        str = new String(bytes, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new SamzaException("Error deserializing data", e);
      }

      try {
        if (clazz != null) {
          return mapper.readValue(str, clazz);
        } else {
          return mapper.readValue(str, new TypeReference<T>() { });
        }
      } catch (Exception e) {
        LOG.debug("Error deserializing data: " + str, e);
        throw new SamzaException("Error deserializing data", e);
      }
    } else {
      return null;
    }
  }
}