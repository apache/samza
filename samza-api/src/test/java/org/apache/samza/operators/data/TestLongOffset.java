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
package org.apache.samza.operators.data;

import org.apache.samza.operators.data.LongOffset;
import org.apache.samza.operators.data.Offset;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;


public class TestLongOffset {

  @Test public void testConstructor() throws Exception {
    LongOffset o1 = new LongOffset("12345");
    Field offsetField = LongOffset.class.getDeclaredField("offset");
    offsetField.setAccessible(true);
    Long x = (Long) offsetField.get(o1);
    assertEquals(x.longValue(), 12345L);

    o1 = new LongOffset("012345");
    x = (Long) offsetField.get(o1);
    assertEquals(x.longValue(), 12345L);

    try {
      o1 = new LongOffset("xyz");
      fail("Constructor of LongOffset should have failed w/ mal-formatted numbers");
    } catch (NumberFormatException nfe) {
      // expected
    }
  }

  @Test public void testComparator() {
    LongOffset o1 = new LongOffset("11111");
    Offset other = mock(Offset.class);
    try {
      o1.compareTo(other);
      fail("compareTo() should have have failed when comparing to an object of a different class");
    } catch (IllegalArgumentException iae) {
      // expected
    }

    LongOffset o2 = new LongOffset("-10000");
    assertEquals(o1.compareTo(o2), 1);
    LongOffset o3 = new LongOffset("22222");
    assertEquals(o1.compareTo(o3), -1);
    LongOffset o4 = new LongOffset("11111");
    assertEquals(o1.compareTo(o4), 0);
  }

  @Test public void testEquals() {
    LongOffset o1 = new LongOffset("12345");
    Offset other = mock(Offset.class);
    assertFalse(o1.equals(other));

    LongOffset o2 = new LongOffset("0012345");
    assertTrue(o1.equals(o2));
  }
}
