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

package org.apache.samza.operators.api.data;

/**
 * An implementation of {@link org.apache.samza.operators.api.data.Offset}, w/ {@code long} value as the offset
 */
public class LongOffset implements Offset {

  /**
   * The offset value in {@code long}
   */
  private final Long offset;

  private LongOffset(long offset) {
    this.offset = offset;
  }

  public LongOffset(String offset) {
    this.offset = Long.valueOf(offset);
  }

  @Override
  public int compareTo(Offset o) {
    if (!(o instanceof LongOffset)) {
      throw new IllegalArgumentException("Not comparable offset classes. LongOffset vs " + o.getClass().getName());
    }
    LongOffset other = (LongOffset) o;
    return this.offset.compareTo(other.offset);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LongOffset)) {
      return false;
    }
    LongOffset o = (LongOffset) other;
    return this.offset.equals(o.offset);
  }

  /**
   * Helper method to get the minimum offset
   *
   * @return The minimum offset
   */
  public static LongOffset getMinOffset() {
    return new LongOffset(Long.MIN_VALUE);
  }

  /**
   * Helper method to get the maximum offset
   *
   * @return The maximum offset
   */
  public static LongOffset getMaxOffset() {
    return new LongOffset(Long.MAX_VALUE);
  }
}
