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

import java.io.Serializable;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskFactory;


/**
 * A {@link TaskApplicationDescriptor} contains the description of inputs, outputs, state, configuration and the
 * processing logic for a Samza Low Level API {@link TaskApplication}.
 * <p>
 * Use the {@link TaskApplicationDescriptor} obtained from {@link TaskApplication#describe}
 * to add the {@link InputDescriptor}s, {@link OutputDescriptor}s and {@link TableDescriptor}s for streams and
 * tables to be used in the task implementation.
 * <p>
 * Use {@link TaskApplicationDescriptor#withTaskFactory} to set the factory for the {@link StreamTask} or
 * {@link AsyncStreamTask} implementation that contains the processing logic for the {@link TaskApplication}.
 */
@InterfaceStability.Evolving
public interface TaskApplicationDescriptor extends ApplicationDescriptor<TaskApplicationDescriptor> {

  /**
   * Sets the {@link StreamTaskFactory} or {@link AsyncStreamTaskFactory} for the {@link StreamTask} or
   * {@link AsyncStreamTask} implementation that contains the processing logic for the {@link TaskApplication}.
   * <p>
   * The provided {@code taskFactory} instance must be {@link Serializable}.
   *
   * @param factory the {@link TaskFactory} for the Low Level API Task implementation
   * @return this {@link TaskApplicationDescriptor}
   */
  TaskApplicationDescriptor withTaskFactory(TaskFactory factory);

  /**
   * Adds the input stream to the application.
   *
   * @param isd the {@link InputDescriptor}
   * @return this {@link TaskApplicationDescriptor}
   */
  TaskApplicationDescriptor withInputStream(InputDescriptor isd);

  /**
   * Adds the output stream to the application.
   *
   * @param osd the {@link OutputDescriptor} of the output stream
   * @return this {@link TaskApplicationDescriptor}
   */
  TaskApplicationDescriptor withOutputStream(OutputDescriptor osd);

  /**
   * Adds the {@link TableDescriptor} used in the application
   *
   * @param table {@link TableDescriptor}
   * @return this {@link TaskApplicationDescriptor}
   */
  TaskApplicationDescriptor withTable(TableDescriptor table);

}