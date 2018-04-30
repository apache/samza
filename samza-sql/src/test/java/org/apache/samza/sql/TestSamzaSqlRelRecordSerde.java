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
import org.apache.samza.sql.serializers.SamzaSqlRelRecordSerdeFactory;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.samza.sql.serializers.SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde;
import static org.apache.samza.sql.data.SamzaSqlRelMessage.SamzaSqlRelRecord;


public class TestSamzaSqlRelRecordSerde {

  private List<Object> values = Arrays.asList("value1", 1, null);
  private List<String> names = Arrays.asList("field1", "field2", "field3");

  @Test
  public void testWithDifferentFields() {
    SamzaSqlRelRecord record = new SamzaSqlRelMessage(names, values).getSamzaSqlRelRecord();
    SamzaSqlRelRecordSerde serde =
        (SamzaSqlRelRecordSerde) new SamzaSqlRelRecordSerdeFactory().getSerde(null, null);
    SamzaSqlRelRecord resultRecord = serde.fromBytes(serde.toBytes(record));
    Assert.assertEquals(names, resultRecord.getFieldNames());
    Assert.assertEquals(values, resultRecord.getFieldValues());
  }
}