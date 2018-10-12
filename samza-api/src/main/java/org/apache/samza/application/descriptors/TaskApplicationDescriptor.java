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
package org.apache.samza.application.descriptors;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.task.TaskFactory;


/**
 *  The interface to describe a {@link org.apache.samza.application.SamzaApplication} that uses low-level API task
 *  for processing.
 */
@InterfaceStability.Evolving
public interface TaskApplicationDescriptor extends ApplicationDescriptor<TaskApplicationDescriptor> {

  /**
   * Sets the {@link TaskFactory} for the user application. The {@link TaskFactory#createInstance()} creates task instance
   * that implements the main processing logic of the user application.
   *
   * @param factory the {@link TaskFactory} including the low-level task processing logic. The only allowed task factory
   *                classes are {@link org.apache.samza.task.StreamTaskFactory} and {@link org.apache.samza.task.AsyncStreamTaskFactory}.
   */
  void setTaskFactory(TaskFactory factory);

  /**
   * Adds the input stream to the application.
   *
   * @param isd the {@link InputDescriptor}
   */
  void addInputStream(InputDescriptor isd);

  /**
   * Adds the output stream to the application.
   *
   * @param osd the {@link OutputDescriptor} of the output stream
   */
  void addOutputStream(OutputDescriptor osd);

  /**
   * Adds the {@link TableDescriptor} used in the application
   *
   * @param table {@link TableDescriptor}
   */
  void addTable(TableDescriptor table);

}