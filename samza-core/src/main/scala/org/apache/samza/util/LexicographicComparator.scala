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

package org.apache.samza.util;

import java.util.Comparator

/**
 * A comparator that applies a lexicographical comparison on byte arrays.
 */
class LexicographicComparator extends Comparator[Array[Byte]] {
  def compare(k1: Array[Byte], k2: Array[Byte]): Int = {
    val l = math.min(k1.length, k2.length)
    var i = 0
    while (i < l) {
      if (k1(i) != k2(i))
        return (k1(i) & 0xff) - (k2(i) & 0xff)
      i += 1
    }
    // okay prefixes are equal, the shorter array is less
    k1.length - k2.length
  }
}
