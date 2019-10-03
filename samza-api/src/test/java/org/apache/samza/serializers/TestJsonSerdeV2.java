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

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class TestJsonSerdeV2 {
  @Test
  public void testJsonSerdeV2ShouldWork() {
    JsonSerdeV2<HashMap<String, Object>> serde = new JsonSerdeV2<>();
    HashMap<String, Object> obj = new HashMap<>();
    obj.put("hi", "bye");
    obj.put("why", 2);
    byte[] bytes = serde.toBytes(obj);
    assertEquals(obj, serde.fromBytes(bytes));
    JsonSerdeV2<Map.Entry<String, Object>> serdeHashMapEntry = new JsonSerdeV2<>();
    obj.entrySet().forEach(entry -> {
        try {
          serdeHashMapEntry.toBytes(entry);
        } catch (Exception e) {
          fail("HashMap Entry serialization failed!");
        }
      });
  }
}
