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

package org.apache.samza.sql.testutil;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility methods to aid serialization and deserialization of Json.
 */
public class JsonUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Logger LOG = LoggerFactory.getLogger(JsonUtil.class.getName());

  static {
    MAPPER.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  private JsonUtil() {
  }

  /**
   * Deserialize a JSON string into an object based on a type reference.
   * This method allows the caller to specify precisely the desired output
   * type for the target object.
   * @param json JSON string
   * @param typeRef type reference of the target object
   * @param <T> type of the target object
   * @return deserialized Java object
   */
  public static <T> T fromJson(String json, TypeReference<T> typeRef) {
    Validate.notNull(json, "null JSON string");
    Validate.notNull(typeRef, "null type reference");
    T object;
    try {
      object = MAPPER.readValue(json, typeRef);
    } catch (IOException e) {
      String errorMessage = "Failed to parse json: " + json;
      LOG.error(errorMessage, e);
      throw new SamzaException(errorMessage, e);
    }
    return object;
  }

  /**
   * Serialize a Java object into JSON string.
   * @param object object to be serialized
   * @param <T> type of the input object
   * @return JSON string
   */
  public static <T> String toJson(T object) {
    Validate.notNull(object, "null input object");
    StringWriter out = new StringWriter();
    try {
      MAPPER.writeValue(out, object);
    } catch (IOException e) {
      String errorMessage = "Failed to serialize object: " + object;
      LOG.error(errorMessage, e);
      throw new SamzaException(errorMessage, e);
    }
    return out.toString();
  }
}
