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

package org.apache.samza.control;

import org.apache.samza.system.SystemStream;


/**
 * A end-of-stream represents a {@link SystemStream} has been fully consumed to the end. There will be
 * no more messages after it of the stream for the current task.
 *
 * <p>This is the aggregate result from the {@link EndOfStreamManager}, which keeps track of the control message
 * {@link org.apache.samza.message.EndOfStreamMessage} and aggregates it for each partition.
 */
public interface EndOfStream {

  /**
   * Returns the {@link SystemStream} that reaches to the end for this task.
   * Note that if the task consumes more than one partitions of this stream, all the partitions are ended.
   * @return the stream that reaches the end
   */
  SystemStream getSystemStream();
}
