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
package org.apache.samza.system.inmemory;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;


public class TestInMemorySystemAdmin {
  @Mock
  private InMemoryManager inMemoryManager;

  private InMemorySystemAdmin inMemorySystemAdmin;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.inMemorySystemAdmin = new InMemorySystemAdmin("system", this.inMemoryManager);
  }

  @Test
  public void testOffsetComparator() {
    assertEquals(0, inMemorySystemAdmin.offsetComparator(null, null).intValue());
    assertEquals(-1, inMemorySystemAdmin.offsetComparator(null, "0").intValue());
    assertEquals(1, inMemorySystemAdmin.offsetComparator("0", null).intValue());
    assertEquals(-1, inMemorySystemAdmin.offsetComparator("0", "1").intValue());
    assertEquals(0, inMemorySystemAdmin.offsetComparator("0", "0").intValue());
    assertEquals(1, inMemorySystemAdmin.offsetComparator("1", "0").intValue());
  }
}