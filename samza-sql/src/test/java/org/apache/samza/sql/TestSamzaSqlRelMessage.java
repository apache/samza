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
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlRelMessage {

  private List<Object> values = Arrays.asList("value1", "value2");
  private List<String> names = Arrays.asList("field1", "field2");

  @Test
  public void testGetField() {
    SamzaSqlRelMessage message = new SamzaSqlRelMessage(names, values);
    Assert.assertEquals(values.get(0), message.getSamzaSqlRelRecord().getField(names.get(0)).get());
    Assert.assertEquals(values.get(1), message.getSamzaSqlRelRecord().getField(names.get(1)).get());
  }

  @Test
  public void testGetNonExistentField() {
    SamzaSqlRelMessage message = new SamzaSqlRelMessage(names, values);
    Assert.assertFalse(message.getSamzaSqlRelRecord().getField("field3").isPresent());
  }
}
