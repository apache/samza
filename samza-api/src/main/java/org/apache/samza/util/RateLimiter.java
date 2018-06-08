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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.task.TaskContext;

/**
 * A rate limiter interface used by Samza components to limit throughput of operations
 * against a resource. Operations against a resource are represented by credits.
 * Resources could be streams, databases, web services, etc.
 *
 * <p>
 * This interface supports two categories of policies: tagged and non-tagged.
 * Tagged rate limiter is used, when further differentiation is required within a resource.
 * For example: messages in a stream may be treated differently depending on the
 * overall situation of processing; or read/write operations to a database.
 * Tagging is the mechanism to allow this differentiation.
 *
 * <p>
 * The following types of invocations are provided
 * <ul>
 *   <li>Block indefinitely until requested credits become available</li>
 *   <li>Block for a provided amount of time, then return available credits</li>
 * </ul>
 *
 */
@InterfaceStability.Unstable
public interface RateLimiter extends Serializable {

  /**
   * Initialize this rate limiter, this method should be called during container initialization.
   *
   * @param config job configuration
   * @param taskContext task context that owns this rate limiter
   */
  void init(Config config, TaskContext taskContext);

  /**
   * Attempt to acquire the provided number of credits, blocks indefinitely until
   * all requested credits become available.
   *
   * @param numberOfCredit requested number of credits
   */
  void acquire(int numberOfCredit);

  /**
   * Attempt to acquire the provided number of credits, blocks for up to provided amount of
   * time for credits to become available. When timeout elapses and not all required credits
   * can be acquired, it returns the number of credits currently available. It may return
   * immediately, if it determines no credits can be acquired during the provided amount time.
   *
   * @param numberOfCredit requested number of credits
   * @param timeout number of time unit to wait
   * @param unit time unit to for timeout
   * @return number of credits acquired
   */
  int acquire(int numberOfCredit, long timeout, TimeUnit unit);

  /**
   * Attempt to acquire the provided number of credits for a number of tags, blocks indefinitely
   * until all requested credits become available
   *
   * @param tagToCreditMap a map of requested number of credits keyed by tag
   */
  void acquire(Map<String, Integer> tagToCreditMap);

  /**
   * Attempt to acquire the provided number of credits for a number of tags, blocks for up to provided amount of
   * time for credits to become available. When timeout elapses and not all required credits
   * can be acquired, it returns the number of credits currently available. It may return
   * immediately, if it determines no credits can be acquired during the provided amount time.
   *
   * @param tagToCreditMap a map of requested number of credits keyed by tag
   * @param timeout number of time unit to wait
   * @param unit time unit to for timeout
   * @return a map of number of credits acquired keyed by tag
   */
  Map<String, Integer> acquire(Map<String, Integer> tagToCreditMap, long timeout, TimeUnit unit);

  /**
   * Get the entire set of tags for which we have configured credits for rate limiting.
   * @return set of supported tags
   */
  Set<String> getSupportedTags();
}
