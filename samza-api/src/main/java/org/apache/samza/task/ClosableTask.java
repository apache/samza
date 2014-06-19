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

package org.apache.samza.task;

/**
 * A ClosableTask augments {@link org.apache.samza.task.StreamTask}, allowing the method implementer to specify
 * code that will be called when the StreamTask is being shut down by the framework, providing to emit final metrics,
 * clean or close resources, etc.  The close method is not guaranteed to be called in event of crash or hard kill
 * of the process.
 */
public interface ClosableTask {
  void close() throws Exception;
}
