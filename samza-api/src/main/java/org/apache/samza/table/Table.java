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
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.TaskContext;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.InitableFunction;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.task.InitableTask;


/**
 * A {@link Table} is an abstraction for data sources that support random access by key. It is an evolution of the
 * existing {@link KeyValueStore} API. It offers support for both local and remote data sources and composition through
 * hybrid tables. For remote data sources, a {@code RemoteTable} provides optimized access with caching, rate-limiting,
 * batching, and retry support.
 * <p>
 * Use a {@link TableDescriptor} to specify the properties of a {@link Table}. For High Level API
 * {@link StreamApplication}s, use {@link StreamApplicationDescriptor#getTable} to obtain the {@link Table} instance for
 * the descriptor that can be used with the {@link MessageStream} operators like {@link MessageStream#sendTo(Table)}.
 * Alternatively, use {@link TaskContext#getUpdatableTable(String)} in {@link InitableFunction#init} to get the table instance
 * for use within operator functions. For Low Level API {@link TaskApplication}s, use {@link TaskContext#getTable}
 * in {@link InitableTask#init} to get the table instance for use within the Task.
 *
 * @param <R> the type of records in the table
 */
@InterfaceStability.Unstable
public interface Table<R> {
}
