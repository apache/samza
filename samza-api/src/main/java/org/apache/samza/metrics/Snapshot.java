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

package org.apache.samza.metrics;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A statistical snapshot of a collection of values
 */
public class Snapshot {
  private final ArrayList<Long> values;
  private final long min;
  private final long max;
  private final double sum;
  private final int size;

  Snapshot(Collection<Long> values) {
    this.values = new ArrayList<>(values.size());

    long max = Long.MIN_VALUE;
    long min = Long.MAX_VALUE;
    double sum = 0;

    for (Long value: values) {
      sum += value;
      this.values.add(value);

      if (value > max) {
        max = value;
      }

      if (value < min) {
        min = value;
      }
    }

    this.sum = sum;
    this.size = this.values.size();

    if (this.size == 0) {
      this.max = 0;
      this.min = 0;
    } else {
      this.max = max;
      this.min = min;
    }
  }

  /**
   * Get the maximum value in the collection
   *
   * @return maximum value
   */
  public long getMax() {
    return max;
  }

  /**
   * Get the minimum value in the collection
   *
   * @return minimum value
   */
  public long getMin() {
    return min;
  }

  /**
   * Get the average of the values in the collection
   *
   * @return average value
   */
  public double getAverage() {
    return size == 0 ? 0 : sum / size;
  }

  /**
   * Get the sum of values in the collection
   *
   * @return sum of the collection
   */
  public double getSum() {
    return sum;
  }

  /**
   * Get the number of values in the collection
   *
   * @return size of the collection
   */
  public int getSize() {
    return size;
  }

  /**
   * Return the entire list of values
   *
   * @return the list of values
   */
  @SuppressWarnings("unchecked")
  public ArrayList<Long> getValues() {
    return (ArrayList<Long>) values.clone();
  }
}
