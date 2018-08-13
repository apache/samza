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
package org.apache.samza.runtime;

/**
 * This interface defines methods that are invoked in different stages of StreamProcessor's lifecycle in local
 * process (i.e. as a standalone process, or a container process in YARN NodeManager). User can implement this interface
 * to instantiate/release shared objects in the local process.
 */
public interface ProcessorLifecycleListener {
  /**
   * User defined initialization before a StreamProcessor is started
   */
  default void beforeStart() {}

  /**
   * User defined callback after a StreamProcessor is started
   *
   */
  default void afterStart() {}

  /**
   * User defined callback before a StreamProcessor is stopped
   *
   */
  default void beforeStop() {}

  /**
   * User defined callback after a StreamProcessor is stopped
   *
   * @param t the error causing the stop of the StreamProcessor. null value of this parameter indicates a successful completion.
   */
  default void afterStop(Throwable t) {}
}
