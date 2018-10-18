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
package org.apache.samza.table;

import org.apache.samza.annotation.InterfaceStability;


/**
 *
 * A {@link Table} is an abstraction for data sources that support random access by key. It is an
 * evolution of the existing {@link org.apache.samza.storage.kv.KeyValueStore} API. It offers support for
 * both local and remote data sources and composition through hybrid tables. For remote data sources,
 * a {@code RemoteTable} provides optimized access with caching, rate-limiting, and retry support.
 * <p>
 * Depending on the implementation, a {@link Table} can be a {@link ReadableTable} or a {@link ReadWriteTable}.
 * <p>
 * Use a {@link org.apache.samza.table.descriptors.TableDescriptor} to specify the properties of a {@link Table}.
 * For High Level API {@link org.apache.samza.application.StreamApplication}s, use
 * {@link org.apache.samza.application.descriptors.StreamApplicationDescriptor#getTable} to obtain
 * the {@link org.apache.samza.table.Table} instance for the descriptor that can be used with the
 * {@link org.apache.samza.operators.MessageStream} operators like
 * {@link org.apache.samza.operators.MessageStream#sendTo(Table)}.
 * Alternatively, use {@link org.apache.samza.context.TaskContext#getTable(String)} in
 * {@link org.apache.samza.operators.functions.InitableFunction#init} to get the table instance for use within
 * operator functions.
 * For Low Level API {@link org.apache.samza.application.TaskApplication}s, use
 * {@link org.apache.samza.context.TaskContext#getTable(String)} in
 * {@link org.apache.samza.task.InitableTask#init} to get the table instance for use within the Task.
 *
 * @param <R> the type of records in the table
 */
@InterfaceStability.Unstable
public interface Table<R> {
}
