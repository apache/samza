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

package org.apache.samza.checkpoint;

import java.util.Objects;
import org.apache.samza.system.SystemStreamPartition;


public class MockStateCheckpointMarker implements StateCheckpointMarker {
  public static final String MOCK_FACTORY_NAME = MockStateCheckpointMarkerFactory.class.getName();

  public final SystemStreamPartition ssp;
  public final String offset;

  public MockStateCheckpointMarker(SystemStreamPartition ssp, String offset) {
    this.ssp = ssp;
    this.offset = offset;
  }

  @Override
  public String getFactoryName() {
    return MOCK_FACTORY_NAME;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;
    MockStateCheckpointMarker that = (MockStateCheckpointMarker) other;
    return Objects.equals(ssp, that.ssp) &&
        Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ssp, offset);
  }

}
