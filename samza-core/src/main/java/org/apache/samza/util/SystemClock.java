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

/**
 * Default implementation of the Clock interface, which uses the real
 * system clock.
 */
public class SystemClock implements Clock {
  private static final SystemClock INSTANCE = new SystemClock();

  private SystemClock() {}

  public static SystemClock instance() {
    return INSTANCE;
  }

  @Override
  public long currentTimeMillis() {
    return System.currentTimeMillis();
  }
}
