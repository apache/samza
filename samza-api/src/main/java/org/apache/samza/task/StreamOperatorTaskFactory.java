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
 * Build {@link AsyncStreamTask} instances.
 * <p>
 * Implementations should return a new instance of {@link AsyncStreamTask} for each {@link #createInstance()} invocation.
 * Note: It returns an {@link AsyncStreamTask} since <code>StreamOperatorTask</code> is not part of samza-api. It is
 * a temporary hack introduced for SAMZA-2172 and will eventually go away with SAMZA-2203
 */
@Deprecated
public interface StreamOperatorTaskFactory extends TaskFactory<AsyncStreamTask> {
}
