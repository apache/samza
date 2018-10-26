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

import org.junit.Assert;
import org.junit.Test;


public class TestConvertToStringUdf {

  private enum LightSwitch {
    On,
    Off
  }

  @Test
  public void testConvertIntegerToString() {
    ConvertToStringUdf convertToStringUdf = new ConvertToStringUdf();
    Assert.assertEquals(convertToStringUdf.execute(10), "10");
  }

  @Test
  public void testConvertLongToString() {
    ConvertToStringUdf convertToStringUdf = new ConvertToStringUdf();
    Assert.assertEquals(convertToStringUdf.execute(10000000000L), "10000000000");
  }

  @Test
  public void testConvertDoubleToString() {
    ConvertToStringUdf convertToStringUdf = new ConvertToStringUdf();
    Assert.assertEquals(convertToStringUdf.execute(10.0000345), "10.0000345");
  }

  @Test
  public void testConvertBooleanToString() {
    ConvertToStringUdf convertToStringUdf = new ConvertToStringUdf();
    Assert.assertEquals(convertToStringUdf.execute(true), "true");
  }

  @Test
  public void testConvertEnumToString() {
    ConvertToStringUdf convertToStringUdf = new ConvertToStringUdf();
    Assert.assertEquals(convertToStringUdf.execute(LightSwitch.On), "On");
  }
}
