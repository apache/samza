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

import java.io.Serializable;
import org.apache.samza.config.Config;


/**
 * This interface class defines the factory method to create an instance of {@link ProcessorLifecycleListener}.
 */
public interface ProcessorLifecycleListenerFactory extends Serializable {
  /**
   * Create an instance of {@link ProcessorLifecycleListener} for the StreamProcessor
   *
   * @param pContext the context of the corresponding StreamProcessor
   * @param config the configuration of the corresponding StreamProcessor
   * @return the {@link ProcessorLifecycleListener} callback object for the StreamProcessor
   */
  ProcessorLifecycleListener createInstance(ProcessorContext pContext, Config config);
}
