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
 *
 */
package org.apache.samza.operators.impl;

/**
 * A key used in {@link TimeSeriesStoreImpl}
 */
public class TimeSeriesKey<K> {

  private final K key;
  private final long timestamp;
  private final int seqNum;

  public TimeSeriesKey(K k, long time, int seq) {
    key = k;
    timestamp = time;
    seqNum = seq;
  }

  public K getKey() {
    return key;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public int getSeqNum() {
    return seqNum;
  }

  @Override
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    result = 31 * result + seqNum;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TimeSeriesKey<?> that = (TimeSeriesKey<?>) o;

    if (timestamp != that.timestamp) return false;
    if (seqNum != that.seqNum) return false;
    return key != null ? key.equals(that.key) : that.key == null;
  }

  @Override
  public String toString() {
    return "TimeSeriesKey{" +
            "key=" + key +
            ", timestamp=" + timestamp +
            ", seqNum=" + seqNum +
            '}';
  }
}

