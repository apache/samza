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

package org.apache.samza.operators.functions;

import org.apache.samza.annotation.InterfaceStability;

/**
 * An interface for functions that can be closed after their execution.
 * <p>
 * Implement {@link #close()} to free resources used during the execution of the function, clean up state etc.
 * <p>
 * Order of closing: {@link Closable}s are closed in the reverse topological order of operators in the
 * {@link org.apache.samza.operators.StreamGraph}. For any two operators A and B in the graph, if operator B
 * consumes results from operator A, then operator B is guaranteed to be closed before operator A.
 */
@InterfaceStability.Unstable
public interface Closable {
  default void close() {}
}
