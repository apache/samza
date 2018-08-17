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

import org.apache.samza.annotation.InterfaceStability;


/**
 * This interface defines methods that are invoked in different stages of StreamProcessor's lifecycle in local
 * process (i.e. as a standalone process, or a container process in YARN NodeManager).
 *
 * <p>
 * User can implement this interface to instantiate/release shared objects in the local process.
 */
@InterfaceStability.Evolving
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
   * User defined callback after a StreamProcessor is stopped successfully
   */
  default void afterStop() {}

  /**
   * User defined callback after a StreamProcessor is stopped with failure
   *
   * @param t the error causing the stop of the StreamProcessor.
   */
  default void afterFailure(Throwable t) {}
}
