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

import java.util.List;

public class MathUtil {

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

  /**
   * Add the supplied arguments and handle overflow by clamping the resulting sum to
   * {@code Long.MinValue} if the sum would have been less than {@code Long.MinValue} or
   * {@code Long.MaxValue} if the sum would have been greater than {@code Long.MaxValue}.
   *
   * @param lhs left hand side of sum
   * @param rhs right hand side of sum
   * @return the sum if no overflow occurs, or the clamped extreme if it does.
   */
  public static long clampAdd(long lhs, long rhs) {
    long sum = lhs + rhs;

    // From "Hacker's Delight", overflow occurs IFF both operands have the same sign and the
    // sign of the sum differs from the operands. Here we're doing a basic bitwise check that
    // collapses 6 branches down to 2. The expression {@code lhs ^ rhs} will have the high-order
    // bit set to true IFF the signs are different.
    if ((~(lhs ^ rhs) & (lhs ^ sum)) < 0) {
      if (lhs >= 0) {
        return Long.MAX_VALUE;
      } else {
        return Long.MIN_VALUE;
      }
    }

    return sum;
  }
}
