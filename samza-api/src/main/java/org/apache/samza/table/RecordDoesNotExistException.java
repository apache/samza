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

/**
 * Custom exception which can be thrown by implementations of {@link org.apache.samza.table.remote.TableWriteFunction}
 * when {@link AsyncReadWriteTable#updateAsync(Object, Object, Object...)} fails due an existing record not being
 * present for the given key. {@link org.apache.samza.operators.MessageStream#sendUpdateTo(Table, Object...)} will
 * attempt to call {@link AsyncReadWriteTable#putAsync(Object, Object, Object...)} instead to insert a new record if a
 * default is provided.
 */
public class RecordDoesNotExistException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public RecordDoesNotExistException() {
    super();
  }

  public RecordDoesNotExistException(String s, Throwable t) {
    super(s, t);
  }

  public RecordDoesNotExistException(String s) {
    super(s);
  }

  public RecordDoesNotExistException(Throwable t) {
    super(t);
  }
}
