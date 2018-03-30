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

package org.apache.samza.container;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An UncaughtExceptionHandler for SamzaContainer that simply executes the configured {@link #runnable}
 * when any thread throws an uncaught exception.
 */
public class SamzaContainerExceptionHandler implements UncaughtExceptionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SamzaContainerExceptionHandler.class);
  private final Runnable runnable;

  public SamzaContainerExceptionHandler(Runnable runnable) {
    this.runnable = runnable;
  }
  /**
   * Method invoked when the given thread terminates due to the
   * given uncaught exception.
   * <p>Any exception thrown by this method will be ignored by the
   * Java Virtual Machine.
   *
   * @param t the thread
   * @param e the exception
   */
  @Override
  public void uncaughtException(Thread t, Throwable e) {
    String msg = String.format("Uncaught exception in thread %s.", t.getName());
    LOGGER.error(msg, e);
    System.err.println(msg);
    e.printStackTrace(System.err);
    try {
      Util.logThreadDump("Thread dump from uncaught exception handler.");
      runnable.run();
    } catch (Throwable throwable) {
      // Ignore to avoid further exception propagation
    }
  }
}
