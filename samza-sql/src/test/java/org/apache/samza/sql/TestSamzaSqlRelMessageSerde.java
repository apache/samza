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

package org.apache.samza.sql;

import java.util.Arrays;
import java.util.List;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlRelMessageSerde {

  private List<Object> values = Arrays.asList("value1", 1, null);
  private List<String> names = Arrays.asList("field1", "field2", "field3");

  @Test
  public void testWithDifferentFields() {
    SamzaSqlRelMessage message = new SamzaSqlRelMessage(names, values);
    JsonSerdeV2<SamzaSqlRelMessage> serde = new JsonSerdeV2<>(SamzaSqlRelMessage.class);
    SamzaSqlRelMessage resultMsg = serde.fromBytes(serde.toBytes(message));
    Assert.assertEquals(resultMsg.getFieldNames(), names);
    Assert.assertEquals(resultMsg.getFieldValues(), values);
  }
}