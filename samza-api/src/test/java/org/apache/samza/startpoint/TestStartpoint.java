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
package org.apache.samza.startpoint;

import org.junit.Assert;
import org.junit.Test;


public class TestStartpoint {

  @Test
  public void TestWithMethods() {
    Startpoint startpoint = Startpoint.withSpecificOffset("123");
    Assert.assertEquals(PositionType.SPECIFIC_OFFSET, startpoint.getPositionType());
    Assert.assertEquals("123", startpoint.getPosition());

    startpoint = Startpoint.withTimestamp(2222222L);
    Assert.assertEquals(PositionType.TIMESTAMP, startpoint.getPositionType());
    Assert.assertEquals("2222222", startpoint.getPosition());

    startpoint = Startpoint.withEarliest();
    Assert.assertEquals(PositionType.EARLIEST, startpoint.getPositionType());
    Assert.assertEquals(null, startpoint.getPosition());

    startpoint = Startpoint.withLatest();
    Assert.assertEquals(PositionType.LATEST, startpoint.getPositionType());
    Assert.assertEquals(null, startpoint.getPosition());

    startpoint = Startpoint.withBootstrap("Bootstrap info");
    Assert.assertEquals(PositionType.BOOTSTRAP, startpoint.getPositionType());
    Assert.assertEquals("Bootstrap info", startpoint.getPosition());
  }
}
