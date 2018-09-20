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


public class TestOutputMessageEnvelope {
  private final String key;
  private final Integer value;

  public TestOutputMessageEnvelope(String key, Integer value) {
    this.key = key;
    this.value = value;
  }

  public Integer getMessage() {
    return this.value;
  }

  public String getKey() {
    return this.key;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TestOutputMessageEnvelope)) {
      return false;
    }
    TestOutputMessageEnvelope otherMsg = (TestOutputMessageEnvelope) other;
    return this.key.equals(otherMsg.key) && this.value.equals(otherMsg.value);
  }

  @Override
  public int hashCode() {
    return String.format("%s:%d", key, value).hashCode();
  }
}

