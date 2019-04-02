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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.junit.Assert;
import org.junit.Test;


public class TestBuildOutputRecordUdf {

  @Test
  public void testNoArgs() {
    BuildOutputRecordUdf buildOutputRecordUdf = new BuildOutputRecordUdf();

    SamzaSqlRelRecord actualRecord = buildOutputRecordUdf.execute();
    SamzaSqlRelRecord expectedRecord =
        new SamzaSqlRelRecord(new ArrayList<>(), new ArrayList<>());

    Assert.assertEquals(actualRecord.getFieldNames(), expectedRecord.getFieldNames());
    Assert.assertEquals(actualRecord.getFieldValues(), expectedRecord.getFieldValues());
  }

  @Test
  public void testSinglePair() {
    BuildOutputRecordUdf buildOutputRecordUdf = new BuildOutputRecordUdf();

    SamzaSqlRelRecord actualRecord = buildOutputRecordUdf.execute("key", "value");
    SamzaSqlRelRecord expectedRecord =
        new SamzaSqlRelRecord(Arrays.asList("key"), Arrays.asList("value"));

    Assert.assertEquals(actualRecord.getFieldNames(), expectedRecord.getFieldNames());
    Assert.assertEquals(actualRecord.getFieldValues(), expectedRecord.getFieldValues());
  }

  @Test
  public void testMultiPairs() {
    BuildOutputRecordUdf buildOutputRecordUdf = new BuildOutputRecordUdf();

    SamzaSqlRelRecord actualRecord = buildOutputRecordUdf.execute("k1", "v1", "k2", "v2");
    SamzaSqlRelRecord expectedRecord =
        new SamzaSqlRelRecord(Arrays.asList("k1", "k2"), Arrays.asList("v1", "v2"));

    Assert.assertEquals(actualRecord.getFieldNames(), expectedRecord.getFieldNames());
    Assert.assertEquals(actualRecord.getFieldValues(), expectedRecord.getFieldValues());
  }

  @Test
  public void testNestedRecord() {
    BuildOutputRecordUdf buildOutputRecordUdf = new BuildOutputRecordUdf();
    SamzaSqlRelRecord nestedSamzaSqlRelRecord =
        new SamzaSqlRelRecord(Arrays.asList("k3"), Arrays.asList("v3"));

    SamzaSqlRelRecord actualRecord = buildOutputRecordUdf.execute("k1", "v1", "k2", nestedSamzaSqlRelRecord);
    SamzaSqlRelRecord expectedRecord =
        new SamzaSqlRelRecord(Arrays.asList("k1", "k2"),
            Arrays.asList("v1", nestedSamzaSqlRelRecord));

    Assert.assertEquals(actualRecord.getFieldNames(), expectedRecord.getFieldNames());
    Assert.assertEquals(actualRecord.getFieldValues(), expectedRecord.getFieldValues());
  }

  @Test(expected = NullPointerException.class)
  public void testNullArgs() {
    BuildOutputRecordUdf buildOutputRecordUdf = new BuildOutputRecordUdf();
    buildOutputRecordUdf.execute(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOddNumOfArgs() {
    BuildOutputRecordUdf buildOutputRecordUdf = new BuildOutputRecordUdf();
    buildOutputRecordUdf.execute("k1");
  }
}
