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

package org.apache.samza.tools;

import java.util.Random;


/**
 * Simple utility to generate random values.
 */
public class RandomValueGenerator {

  private String validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  private Random rand;

  // to help reproducibility of failed tests, seed is always required
  public RandomValueGenerator(long seed) {
    rand = new Random(seed);
  }

  public int getNextInt() {
    return rand.nextInt();
  }

  // to make it inclusive of min and max for the range, add 1 to the difference
  public int getNextInt(int min, int max) {
    if (max == min) {
      return min;
    }
    // assert(max > min);

    return (rand.nextInt(max - min + 1) + min);
  }

  public String getNextString(int min, int max) {
    int length = getNextInt(min, max);

    StringBuilder strbld = new StringBuilder();
    for (int i = 0; i < length; i++) {
      char ch = validChars.charAt(rand.nextInt(validChars.length()));
      strbld.append(ch);
    }

    return strbld.toString();
  }

  public double getNextDouble() {
    return rand.nextDouble();
  }

  public float getNextFloat() {
    return rand.nextFloat();
  }

  public long getNextLong() {
    long randomLong = rand.nextLong();

    return randomLong == Long.MIN_VALUE ? 0 : Math.abs(randomLong);
  }

  public boolean getNextBoolean() {
    return rand.nextBoolean();
  }

  public byte[] getNextBytes(int maxBytesLength) {
    byte[] bytes = new byte[this.getNextInt(0, maxBytesLength)];
    rand.nextBytes(bytes);
    return bytes;
  }
}
