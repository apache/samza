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
package org.apache.samza.logging;

import java.util.concurrent.atomic.AtomicReference;
import com.google.common.annotations.VisibleForTesting;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;


/**
 * Holds information to be used by loggers. For example, some custom Samza log4j/log4j2 logging appenders need system
 * configs for initialization, so this allows the configs to be passed to those appenders.
 */
public class LoggingContextHolder {
  public static final LoggingContextHolder INSTANCE = new LoggingContextHolder();

  private final AtomicReference<Config> config = new AtomicReference<>();

  @VisibleForTesting
  LoggingContextHolder() {
  }

  /**
   * Set the config. If this is called multiple times, then a {@link SamzaException} will be thrown.
   */
  public void setConfig(Config config) {
    if (!this.config.compareAndSet(null, config)) {
      throw new SamzaException("Attempted to set config, but it was already set");
    }
  }

  public Config getConfig() {
    return this.config.get();
  }
}
