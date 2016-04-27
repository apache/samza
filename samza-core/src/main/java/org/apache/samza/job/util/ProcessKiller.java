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
package org.apache.samza.job.util;

import java.lang.reflect.Method;


/**
 * Kills a {@link Process} consistently, independent of Java version.
 */
public class ProcessKiller {
  /**
   * Force-kills the process independently of Java implementation.
   *
   * In Java 7, destroy() would force kill the process.
   *
   * Java 8 changed the behavior of destroy() to be normal
   * termination and added destroyForcibly(), sigh.
   *
   * TODO: remove this class when Java 7 is no longer supported.
   *
   * @param process the process to destroy.
   */
  public static void destroyForcibly(Process process) {
    try {
      Method methodToFind = Process.class.getMethod("destroyForcibly", (Class<?>[]) null);
      methodToFind.invoke(process);
    } catch (Exception e) {
      process.destroy();
    }
  }
}
