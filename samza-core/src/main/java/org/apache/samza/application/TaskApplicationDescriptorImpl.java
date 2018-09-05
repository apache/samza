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

import org.apache.samza.config.Config;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.task.TaskFactory;


/**
 * This class implements interface {@link TaskApplicationDescriptor}.
 * <p>
 * In addition to the common objects for an application defined in {@link ApplicationDescriptorImpl}, this class also includes
 * the low-level {@link TaskFactory} that creates user-defined task instances, the lists of input/broadcast/output streams,
 * and the list of {@link TableDescriptor}s used in the application.
 */
public class TaskApplicationDescriptorImpl extends ApplicationDescriptorImpl<TaskApplicationDescriptor>
    implements TaskApplicationDescriptor {

  TaskFactory taskFactory;

  public TaskApplicationDescriptorImpl(TaskApplication userApp, Config config) {
    super(userApp, config);
    userApp.describe(this);
  }

  @Override
  public void setTaskFactory(TaskFactory factory) {
    this.taskFactory = factory;
  }

  @Override
  public void addInputStream(InputDescriptor isd) {
    addInputDescriptor(isd);
  }

  @Override
  public void addOutputStream(OutputDescriptor osd) {
    addOutputDescriptor(osd);
  }

  @Override
  public void addTable(TableDescriptor tableDescriptor) {
    addTableDescriptor(tableDescriptor);
  }

  /**
   * Get the user-defined {@link TaskFactory}
   * @return the {@link TaskFactory} object
   */
  public TaskFactory getTaskFactory() {
    return taskFactory;
  }

  @Override
  protected boolean noInputOutputStreams() {
    return getInputDescriptors().isEmpty() && getOutputDescriptors().isEmpty();
  }
}