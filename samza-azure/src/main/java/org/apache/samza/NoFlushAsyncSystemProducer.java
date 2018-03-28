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

package org.apache.samza;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper System producer that can be used to implement system producers whose APIs don't provide flush semantics
 * but instead provides Async callbacks e.g. Event Hubs
 */
public abstract class NoFlushAsyncSystemProducer extends AsyncSystemProducer {

  private static final Logger LOG = LoggerFactory.getLogger(NoFlushAsyncSystemProducer.class.getName());
  private static final long DEFAULT_FLUSH_TIMEOUT_MILLIS = Duration.ofMinutes(1L).toMillis();

  public NoFlushAsyncSystemProducer(String systemName, Config config, MetricsRegistry registry) {
    super(systemName, config, registry);
  }

  @Override
  public void flush(String source) {
    long incompleteSends = pendingFutures.stream().filter(x -> !x.isDone()).count();
    LOG.info("Trying to flush pending {} sends.", incompleteSends);
    checkCallbackThrowable("Received exception on message send.");
    CompletableFuture<Void> future =
        CompletableFuture.allOf(pendingFutures.toArray(new CompletableFuture[pendingFutures.size()]));

    try {
      // Block until all the pending sends are complete or timeout.
      future.get(DEFAULT_FLUSH_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      incompleteSends = pendingFutures.stream().filter(x -> !x.isDone()).count();
      String msg = String.format("Flush failed with error. Total pending sends %d", incompleteSends);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }

    pendingFutures.clear();

    checkCallbackThrowable("Sending one or more of the messages failed during flush.");
  }
}
