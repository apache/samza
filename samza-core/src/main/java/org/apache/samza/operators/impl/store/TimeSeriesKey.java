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
package org.apache.samza.operators.impl.store;

/**
 * The store key used in the {@link TimeSeriesStore} to uniquely identify a row.
 */
public class TimeSeriesKey<K> {

  // version for backwards compatibility
  private static final byte VERSION = 0x00;
  private final K key;
  private final long timestamp;

  private final long seqNum;

  public TimeSeriesKey(K k, long time, long seq) {
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

  public byte getVersion() {
    return VERSION;
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
  public int hashCode() {
    int result = key != null ? key.hashCode() : 0;
    result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
    result = 31 * result + (int) (seqNum ^ (seqNum >>> 32));
    return result;
  }

  public long getSeqNum() {
    return seqNum;
  }


  @Override
  public String toString() {
    return String.format("TimeSeriesKey {key: %s timestamp: %s seqNum: %s}", key, timestamp, seqNum);
  }
}
