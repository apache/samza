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
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Holds information to be used by loggers. For example, some custom Samza log4j/log4j2 logging appenders need system
 * configs for initialization, so this allows the configs to be passed to those appenders.
 */
public class LoggingContextHolder {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingContextHolder.class);
  public static final LoggingContextHolder INSTANCE = new LoggingContextHolder();

  private final AtomicReference<Config> config = new AtomicReference<>();

  @VisibleForTesting
  LoggingContextHolder() {
  }

  /**
   * Set the config to be used by Samza loggers.
   * Only the config used in the first call to this method will be used. After the first call, this method will do
   * nothing.
   */
  public void setConfig(Config config) {
    if (!this.config.compareAndSet(null, config)) {
      LOG.warn("Attempted to set config, but it was already set");
    }
  }

  public Config getConfig() {
    return this.config.get();
  }
}
