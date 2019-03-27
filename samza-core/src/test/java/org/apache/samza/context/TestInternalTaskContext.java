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
package org.apache.samza.context;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestInternalTaskContext {

  private InternalTaskContext internalTaskContext;

  @Before
  public void setup() {
    internalTaskContext = new InternalTaskContext(null);
  }

  /**
   * Given a registered object, fetchObject should get it. If an object is not registered at a key, then fetchObject
   * should return null.
   */
  @Test
  public void testRegisterAndFetchObject() {
    String value = "hello world";
    internalTaskContext.registerObject("key", value);
    assertEquals(value, internalTaskContext.fetchObject("key"));
    assertNull(internalTaskContext.fetchObject("not a key"));
  }

}