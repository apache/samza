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

import com.google.common.primitives.UnsignedBytes;

import java.util.Comparator;

/**
 * A wrapper over byte array that supports lexicographic comparisons.
 */
public class Bytes implements Comparable<Bytes> {

  private final byte[] bytes;

  Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();

  private Bytes(byte[] byteArray) {
    bytes = byteArray;
  }

  public static Bytes wrap(byte[] bytes) {
    return new Bytes(bytes);
  }

  public byte[] get() {
    return this.bytes;
  }

  @Override
  public int compareTo(Bytes other) {
    return comparator.compare(this.bytes, other.bytes);
  }
}
