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

import org.apache.samza.application.TaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.table.descriptors.BaseTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
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
  private TaskFactory taskFactory = null;

  public TaskApplicationDescriptorImpl(TaskApplication userApp, Config config) {
    super(userApp, config);
    userApp.describe(this);
  }

  @Override
  public TaskApplicationDescriptor withTaskFactory(TaskFactory factory) {
    this.taskFactory = factory;
    return this;
  }

  @Override
  public TaskApplicationDescriptor withInputStream(InputDescriptor inputDescriptor) {
    // TODO: SAMZA-1841: need to add to the broadcast streams if inputDescriptor is for a broadcast stream
    addInputDescriptor(inputDescriptor);
    getOrCreateStreamSerdes(inputDescriptor.getStreamId(), inputDescriptor.getSerde());
    return this;
  }

  @Override
  public TaskApplicationDescriptor withOutputStream(OutputDescriptor outputDescriptor) {
    addOutputDescriptor(outputDescriptor);
    getOrCreateStreamSerdes(outputDescriptor.getStreamId(), outputDescriptor.getSerde());
    return this;
  }

  @Override
  public TaskApplicationDescriptor withTable(TableDescriptor tableDescriptor) {
    addTableDescriptor(tableDescriptor);
    BaseTableDescriptor baseTableDescriptor = (BaseTableDescriptor) tableDescriptor;
    getOrCreateTableSerdes(baseTableDescriptor.getTableId(), baseTableDescriptor.getSerde());
    return this;
  }

  /**
   * Get the user-defined {@link TaskFactory}
   * @return the {@link TaskFactory} object
   */
  public TaskFactory getTaskFactory() {
    return taskFactory;
  }
}