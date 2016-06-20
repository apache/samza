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

import java.util.concurrent.TimeUnit;

class SystemHighResolutionClock implements HighResolutionClock {
  @Override
  public long nanoTime() {
    return System.nanoTime();
  }

  @Override
  public long sleep(long nanos) throws InterruptedException {
    if (nanos <= 0) {
      return nanos;
    }

    final long start = System.nanoTime();
    TimeUnit.NANOSECONDS.sleep(nanos);

    return Util.clampAdd(nanos, -(System.nanoTime() - start));
  }
}
