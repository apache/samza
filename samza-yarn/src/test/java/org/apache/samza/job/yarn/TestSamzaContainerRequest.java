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
package org.apache.samza.job.yarn;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestSamzaContainerRequest {
  private static final String ANY_HOST = ContainerRequestState.ANY_HOST;

  @Test
  public void testPreferredHostIsNeverNull() {
    SamzaContainerRequest request = new SamzaContainerRequest(0, null);

    assertNotNull(request.getPreferredHost());

    // preferredHost is null, it should automatically default to ANY_HOST
    assertTrue(request.getPreferredHost().equals(ANY_HOST));

    SamzaContainerRequest request1 = new SamzaContainerRequest(1, "abc");
    assertNotNull(request1.getPreferredHost());
    assertTrue(request1.getPreferredHost().equals("abc"));

  }
}
