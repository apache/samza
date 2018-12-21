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

import java.time.Instant;


public class MockStartpointCustom extends StartpointCustom {
  private final String testInfo1;
  private final long testInfo2;

  // Default constructor needed for serde.
  private MockStartpointCustom() {
    this(null, 0);
  }

  public MockStartpointCustom(String testInfo1, long testInfo2) {
    this(testInfo1, testInfo2, Instant.now().toEpochMilli());
  }

  public MockStartpointCustom(String testInfo1, long testInfo2, long creationTimestamp) {
    super(creationTimestamp);
    this.testInfo1 = testInfo1;
    this.testInfo2 = testInfo2;
  }

  public String getTestInfo1() {
    return testInfo1;
  }

  public long getTestInfo2() {
    return testInfo2;
  }
}
