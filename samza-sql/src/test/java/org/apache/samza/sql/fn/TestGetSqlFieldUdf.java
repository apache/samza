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

package org.apache.samza.sql.fn;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.junit.Assert;
import org.junit.Test;


public class TestGetSqlFieldUdf {
  static Object createRecord(List<String> fieldNames, int level) {
    String fieldName = fieldNames.get(level);
    Object child = (level == fieldNames.size() - 1) ? "bar" : createRecord(fieldNames, level + 1);
    boolean isMap = false;
    int arrayIndex = -1;
    if (fieldName.startsWith("map:")) {
      isMap = true;
      fieldName = fieldName.substring(4); // strip "map:"
    } else if (fieldName.endsWith("]")) {
      arrayIndex = Integer.parseInt(fieldName.substring(fieldName.indexOf("[") + 1, fieldName.length() - 1));
      fieldName = fieldName.substring(0, fieldName.indexOf("["));
    }

    if (isMap) {
      Map<String, Object> retMap = new HashMap<>();
      retMap.put(fieldName, child);
      return retMap;
    } else if (arrayIndex >= 0) {
      List list = Arrays.asList(new Object[2 * arrayIndex]);
      list.set(arrayIndex, child);
      return list;
    } else {
      return new SamzaSqlRelRecord(Collections.singletonList(fieldName), Collections.singletonList(child));
    }
  }

  private SamzaSqlRelRecord createRecord(String path) {
    return (SamzaSqlRelRecord) createRecord(Arrays.asList(path.split("\\.")), 0);
  }

  @Test
  public void testSingleLevel() {
    SamzaSqlRelRecord record = createRecord("foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(record, "foo"), "bar");
  }

  @Test
  public void testMultiLevel() {
    SamzaSqlRelRecord record = createRecord("bar.baz.baf.foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(record, "bar.baz.baf.foo"), "bar");
  }

  @Test
  public void testNullRecord() {
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(null, "bar.baz.baf.foo"), null);
  }

  @Test (expected = NullPointerException.class)
  public void testNullFields() {
    SamzaSqlRelRecord record = createRecord("bar.baz.baf.foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    getSqlFieldUdf.execute(record, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSingleLevelInvalidField() {
    SamzaSqlRelRecord record = createRecord("foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    getSqlFieldUdf.execute(record, "bar");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultiLevelInvalidIntermediateField() {
    SamzaSqlRelRecord record = createRecord("bar.baz.baf.foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    getSqlFieldUdf.execute(record, "bar.baz.bacon");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultiLevelInvalidFinalField() {
    SamzaSqlRelRecord record = createRecord("bar.baz.baf.foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    getSqlFieldUdf.execute(record, "bar.baz.baf.funny");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPathTooDeep() {
    SamzaSqlRelRecord record = createRecord("bar.foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    getSqlFieldUdf.execute(record, "bar.baz.baf.funny");
  }

  @Test
  public void testMapAtLastField() {
    SamzaSqlRelRecord record = createRecord("bar.baz.baf.map:foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(record, "bar.baz.baf.foo"), "bar");
  }

  @Test
  public void testMapAtIntermediateFields() {
    SamzaSqlRelRecord record = createRecord("bar.map:baz.map:baf.foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(record, "bar.baz.baf.foo"), "bar");
  }

  @Test
  public void testMapAtAllIntermediateFields() {
    SamzaSqlRelRecord record = createRecord("bar.map:baz.map:baf.map:foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(record, "bar.baz.baf.foo"), "bar");
  }

  @Test
  public void testArrayAtLastField() {
    SamzaSqlRelRecord record = createRecord("bar.baz.baf.foo[3]");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(record, "bar.baz.baf.foo[3]"), "bar");
  }

  @Test
  public void testArrayAtIntermediateFields() {
    SamzaSqlRelRecord record = createRecord("bar.baz[3].baf[2].foo");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(record, "bar.baz[3].baf[2].foo"), "bar");
  }

  @Test
  public void testArrayAtAllIntermediateFields() {
    SamzaSqlRelRecord record = createRecord("bar.baz[2].baf[3].foo[5]");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(record, "bar.baz[2].baf[3].foo[5]"), "bar");
  }

  @Test
  public void testAllFieldTypes() {
    SamzaSqlRelRecord record = createRecord("bar.map:baz.baf.foo[3].fun");
    GetSqlFieldUdf getSqlFieldUdf = new GetSqlFieldUdf();
    Assert.assertEquals(getSqlFieldUdf.execute(record, "bar.baz.baf.foo[3].fun"), "bar");
  }
}
