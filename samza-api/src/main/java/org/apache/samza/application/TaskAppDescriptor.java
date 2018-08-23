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
package org.apache.samza.application;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.task.TaskFactory;


/**
 *  The interface class to describe a user application as low-level task in Samza.
 */
@InterfaceStability.Unstable
public interface TaskAppDescriptor extends ApplicationDescriptor<TaskAppDescriptor> {

  /**
   * Sets the {@link TaskFactory} for the user application. The {@link TaskFactory#createInstance()} creates task instance
   * that implements the main processing logic of the user application.
   *
   * @param factory the user implemented {@link TaskFactory} including the low-level task processing logic. The only
   *                allowed task factor classes are {@link org.apache.samza.task.StreamTaskFactory} and {@link org.apache.samza.task.AsyncStreamTaskFactory}.
   */
  void setTaskFactory(TaskFactory factory);

  /**
   * Adds the input stream to the user application.
   *
   * @param inputStream streamId of the input stream
   */
  // TODO: needs to be replaced by InputStreamDescriptor after SAMZA-1804 is implemented
  void addInputStream(String inputStream);

  /**
   * Adds the output stream to the user application.
   *
   * @param outputStream streamId of the output stream
   */
  // TODO: needs to be replaced by OutputStreamDescriptor after SAMZA-1804 is implemented
  void addOutputStream(String outputStream);

  /**
   * Adds the {@link TableDescriptor} used in the application
   *
   * @param table {@link TableDescriptor}
   */
  void addTable(TableDescriptor table);

}