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

package org.apache.samza.table.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Modifier;
import java.util.Base64;

import org.apache.samza.SamzaException;


public final class SerdeUtils {
  /**
   * Helper method to serialize Java objects as Base64 strings
   * @param name name of the object (for error reporting)
   * @param object object to be serialized
   * @return Base64 representation of the object
   * @param <T> type of the object
   */
  public static <T> String serialize(String name, T object) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(object);
      return Base64.getEncoder().encodeToString(baos.toByteArray());
    } catch (IOException e) {
      throw new SamzaException("Failed to serialize " + name, e);
    }
  }

  /**
   * Helper method to deserialize Java objects from Base64 strings
   * @param name name of the object (for error reporting)
   * @param strObject base64 string of the serialized object
   * @return deserialized object instance
   * @param <T> type of the object
   */
  @SuppressWarnings("unchecked")
  public static <T> T deserialize(String name, String strObject) {
    try {
      byte[] bytes = Base64.getDecoder().decode(strObject);
      return (T) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
    } catch (Exception e) {
      String errMsg = "Failed to deserialize " + name;
      throw new SamzaException(errMsg, e);
    }
  }

  /**
   * Helper method to serialize Java objects as json strings
   * @param name name of object used for logging
   * @param object object to be serialized
   * @return Json representation of the object
   */
  public static String toJson(String name, Object object) {
    final Gson gson = new GsonBuilder().excludeFieldsWithModifiers(Modifier.TRANSIENT, Modifier.STATIC)
        // Tells Gson how to serialize fields with type of Class.
        .registerTypeHierarchyAdapter(Class.class, new TypeAdapter<Class>() {
          @Override
          public void write(JsonWriter out, Class value) throws IOException {
            if (value == null) {
              out.nullValue();
            } else {
              out.value(value.getName());
            }
          }

          @Override
          public Class read(JsonReader in) {
            throw new SamzaException("Deserialization from json is not supported.");
          }
        }).create();
    try {
      return gson.toJson(object);
    } catch (Exception e) {
      throw new SamzaException(String.format("Failed to serialize %s to json", name), e);
    }
  }
}
