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
package org.apache.samza.util

import org.apache.samza.SamzaException

/**
 * KafkaCheckpointManager handles retries, so we need two kinds of exceptions:
 * one to signal a hard failure, and the other to retry. The
 * KafkaUtilException is thrown to indicate a hard failure that the Kafka
 * CheckpointManager can't recover from.
 */
class KafkaUtilException(var message: String, t: Throwable) extends SamzaException(message, t) {
  def this(s: String) = this(s, null)
}