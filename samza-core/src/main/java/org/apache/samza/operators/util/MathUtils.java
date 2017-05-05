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

package org.apache.samza.operators.util;

import java.util.List;

public class MathUtils {

  public static long gcd(long a, long b) {
    // use the euclid gcd algorithm
    while (b > 0) {
      long temp = b;
      b = a % b;
      a = temp;
    }
    return a;
  }

  public static long gcd(List<Long> numbers) {
    if (numbers == null) {
      throw new IllegalArgumentException("Null list provided");
    }
    if (numbers.size() == 0) {
      throw new IllegalArgumentException("List of size 0 provided");
    }

    long result = numbers.get(0);
    for (int i = 1; i < numbers.size(); i++) {
      result = gcd(result, numbers.get(i));
    }
    return result;
  }
}
