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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.config.Config;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.descriptors.base.stream.InputDescriptor;
import org.apache.samza.operators.descriptors.base.stream.OutputDescriptor;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
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

  private final Map<String, InputDescriptor> inputDescriptors = new LinkedHashMap<>();
  private final Map<String, OutputDescriptor> outputDescriptors = new LinkedHashMap<>();
  private final Set<String> broadcastStreams = new HashSet<>();
  private final Map<String, TableDescriptor> tableDescriptors = new LinkedHashMap<>();
  private final Map<String, SystemDescriptor> systemDescriptors = new LinkedHashMap<>();

  private TaskFactory taskFactory = null;

  public TaskApplicationDescriptorImpl(TaskApplication userApp, Config config) {
    super(userApp, config);
    userApp.describe(this);
  }

  @Override
  public void setTaskFactory(TaskFactory factory) {
    this.taskFactory = factory;
  }

  @Override
  public void addInputStream(InputDescriptor inputDescriptor) {
    // TODO: SAMZA-1841: need to add to the broadcast streams if inputDescriptor is for a broadcast stream
    Preconditions.checkState(!inputDescriptors.containsKey(inputDescriptor.getStreamId()),
        String.format("add input descriptors multiple times with the same streamId: %s", inputDescriptor.getStreamId()));
    getOrCreateStreamSerdes(inputDescriptor.getStreamId(), inputDescriptor.getSerde());
    inputDescriptors.put(inputDescriptor.getStreamId(), inputDescriptor);
    addSystemDescriptor(inputDescriptor.getSystemDescriptor());
  }

  @Override
  public void addOutputStream(OutputDescriptor outputDescriptor) {
    Preconditions.checkState(!outputDescriptors.containsKey(outputDescriptor.getStreamId()),
        String.format("add output descriptors multiple times with the same streamId: %s", outputDescriptor.getStreamId()));
    getOrCreateStreamSerdes(outputDescriptor.getStreamId(), outputDescriptor.getSerde());
    outputDescriptors.put(outputDescriptor.getStreamId(), outputDescriptor);
    addSystemDescriptor(outputDescriptor.getSystemDescriptor());
  }

  @Override
  public void addTable(TableDescriptor tableDescriptor) {
    Preconditions.checkState(!tableDescriptors.containsKey(tableDescriptor.getTableId()),
        String.format("add table descriptors multiple times with the same tableId: %s", tableDescriptor.getTableId()));
    getOrCreateTableSerdes(tableDescriptor.getTableId(), ((BaseTableDescriptor) tableDescriptor).getSerde());
    tableDescriptors.put(tableDescriptor.getTableId(), tableDescriptor);
  }

  @Override
  public Map<String, InputDescriptor> getInputDescriptors() {
    return Collections.unmodifiableMap(inputDescriptors);
  }

  @Override
  public Map<String, OutputDescriptor> getOutputDescriptors() {
    return Collections.unmodifiableMap(outputDescriptors);
  }

  @Override
  public Set<String> getBroadcastStreams() {
    return Collections.unmodifiableSet(broadcastStreams);
  }

  @Override
  public Set<TableDescriptor> getTableDescriptors() {
    return Collections.unmodifiableSet(new HashSet<>(tableDescriptors.values()));
  }

  @Override
  public Set<SystemDescriptor> getSystemDescriptors() {
    // We enforce that users must not use different system descriptor instances for the same system name
    // when getting an input/output stream or setting the default system descriptor
    return Collections.unmodifiableSet(new HashSet<>(systemDescriptors.values()));
  }

  @Override
  public Set<String> getInputStreamIds() {
    return Collections.unmodifiableSet(new HashSet<>(inputDescriptors.keySet()));
  }

  @Override
  public Set<String> getOutputStreamIds() {
    return Collections.unmodifiableSet(new HashSet<>(outputDescriptors.keySet()));
  }

  /**
   * Get the user-defined {@link TaskFactory}
   * @return the {@link TaskFactory} object
   */
  public TaskFactory getTaskFactory() {
    return taskFactory;
  }

  // check uniqueness of the {@code systemDescriptor} and add if it is unique
  private void addSystemDescriptor(SystemDescriptor systemDescriptor) {
    Preconditions.checkState(!systemDescriptors.containsKey(systemDescriptor.getSystemName())
            || systemDescriptors.get(systemDescriptor.getSystemName()) == systemDescriptor,
        "Must not use different system descriptor instances for the same system name: " + systemDescriptor.getSystemName());
    systemDescriptors.put(systemDescriptor.getSystemName(), systemDescriptor);
  }
}