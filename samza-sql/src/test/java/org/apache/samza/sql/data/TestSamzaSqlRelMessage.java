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

package org.apache.samza.sql.data;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.junit.Assert;
import org.junit.Test;


public class TestSamzaSqlRelMessage {

  private List<Object> values = Arrays.asList("value1", "value2");
  private List<String> names = Arrays.asList("field1", "field2");

  @Test
  public void testGetField() {
    SamzaSqlRelMessage message = new SamzaSqlRelMessage(names, values, new SamzaSqlRelMsgMetadata("", "", ""));
    Assert.assertEquals(values.get(0), message.getSamzaSqlRelRecord().getField(names.get(0)).get());
    Assert.assertEquals(values.get(1), message.getSamzaSqlRelRecord().getField(names.get(1)).get());
  }

  @Test
  public void testGetNonExistentField() {
    SamzaSqlRelMessage message = new SamzaSqlRelMessage(names, values, new SamzaSqlRelMsgMetadata("", "", ""));
    Assert.assertFalse(message.getSamzaSqlRelRecord().getField("field3").isPresent());
  }

  @Test
  public void testEquality() {
    SamzaSqlRelMessage message1 = new SamzaSqlRelMessage(names, values, new SamzaSqlRelMsgMetadata("", "", ""));
    SamzaSqlRelMessage message2 =
        new SamzaSqlRelMessage(Arrays.asList("field1", "field2"), Arrays.asList("value1", "value2"),
            new SamzaSqlRelMsgMetadata("", "", ""));
    Assert.assertEquals(message1, message2);
    Assert.assertEquals(message1.hashCode(), message2.hashCode());
  }

  @Test
  public void testInEquality() {
    SamzaSqlRelMessage message1 = new SamzaSqlRelMessage(names, values, new SamzaSqlRelMsgMetadata("", "", ""));
    SamzaSqlRelMessage message2 =
        new SamzaSqlRelMessage(Arrays.asList("field1", "field2"), Arrays.asList("value2", "value2"),
            new SamzaSqlRelMsgMetadata("", "", ""));
    Assert.assertNotEquals(message1, message2);
    Assert.assertNotEquals(message1.hashCode(), message2.hashCode());
  }

  @Test
  public void testCompositeKeyCreation() {
    List<String> keyPartNames = Arrays.asList("kfield1", "kfield2");
    SamzaSqlRelMessage message = new SamzaSqlRelMessage(names, values, new SamzaSqlRelMsgMetadata("", "", ""));

    SamzaSqlRelRecord relRecord1 = SamzaSqlRelMessage.createSamzaSqlCompositeKey(message, Collections.singletonList(0));
    Assert.assertEquals(relRecord1.getFieldNames().size(), 1);
    Assert.assertEquals(relRecord1.getFieldNames().get(0), "field1");
    Assert.assertEquals(relRecord1.getFieldValues().get(0), "value1");

    SamzaSqlRelRecord relRecord2 = SamzaSqlRelMessage.createSamzaSqlCompositeKey(message, Arrays.asList(1, 0),
        SamzaSqlRelMessage.getSamzaSqlCompositeKeyFieldNames(keyPartNames, Arrays.asList(1, 0)));
    Assert.assertEquals(relRecord2.getFieldNames().size(), 2);
    Assert.assertEquals(relRecord2.getFieldNames().get(0), "kfield2");
    Assert.assertEquals(relRecord2.getFieldValues().get(0), "value2");
    Assert.assertEquals(relRecord2.getFieldNames().get(1), "kfield1");
    Assert.assertEquals(relRecord2.getFieldValues().get(1), "value1");
  }

  @Test (expected = IllegalArgumentException.class)
  public void testCompositeKeyCreationWithInEqualKeyNameValues() {
    List<String> keyPartNames = Arrays.asList("kfield1", "kfield2");
    SamzaSqlRelMessage message = new SamzaSqlRelMessage(names, values, new SamzaSqlRelMsgMetadata("", "", ""));

    SamzaSqlRelRecord relRecord1 = SamzaSqlRelMessage.createSamzaSqlCompositeKey(message, Arrays.asList(1, 0),
        SamzaSqlRelMessage.getSamzaSqlCompositeKeyFieldNames(keyPartNames, Arrays.asList(1)));
  }
}
