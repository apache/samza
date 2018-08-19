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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.task.TaskFactory;


/**
 * This class implements interface {@link TaskAppDescriptor}.
 * <p>
 * In addition to the common objects for an application defined in {@link AppDescriptorImpl}, this class also includes
 * the low-level {@link TaskFactory} that creates user-defined task instances, the lists of input/broadcast/output streams,
 * and the list of {@link TableDescriptor}s used in the application.
 */
public class TaskAppDescriptorImpl extends AppDescriptorImpl<TaskApplication, TaskAppDescriptor>
    implements TaskAppDescriptor {

  TaskFactory taskFactory;
  //TODO: need to replace with InputStreamDescriptor and OutputStreamDescriptor when SAMZA-1804 is implemented
  final List<String> inputStreams = new ArrayList<>();
  final List<String> outputStreams = new ArrayList<>();
  final List<String> broadcastStreams = new ArrayList<>();
  final List<TableDescriptor> tables = new ArrayList<>();

  public TaskAppDescriptorImpl(TaskApplication userApp, Config config) {
    super(userApp, config);
    userApp.describe(this);
  }

  @Override
  public void setTaskFactory(TaskFactory factory) {
    this.taskFactory = factory;
  }

  @Override
  public void addInputStream(String inputStream) {
    this.inputStreams.add(inputStream);
  }

  @Override
  public void addBroadcastStream(String broadcastStream) {
    this.broadcastStreams.add(broadcastStream);
  }

  @Override
  public void addOutputStream(String outputStream) {
    this.outputStreams.add(outputStream);
  }

  @Override
  public void addTable(TableDescriptor table) {
    this.tables.add(table);
  }

  /**
   * Get the user-defined {@link TaskFactory}
   * @return the {@link TaskFactory} object
   */
  public TaskFactory getTaskFactory() {
    return taskFactory;
  }

  /**
   * Get the input streams to this application
   *
   * TODO: need to change to InputStreamDescriptors after SAMZA-1804
   *
   * @return the list of input streamIds
   */
  public List<String> getInputStreams() {
    return Collections.unmodifiableList(this.inputStreams);
  }

  /**
   * Get the broadcast streams to this application
   *
   * @return the list of broadcast streamIds
   */
  public List<String> getBroadcastStreams() {
    return Collections.unmodifiableList(this.broadcastStreams);
  }

  /**
   * Get the output streams to this application
   *
   * TODO: need to change to OutputStreamDescriptors after SAMZA-1804
   *
   * @return the list of output streamIds
   */
  public List<String> getOutputStreams() {
    return Collections.unmodifiableList(this.outputStreams);
  }

  /**
   * Get the {@link TableDescriptor}s used in this application
   *
   * @return the list of {@link TableDescriptor}s
   */
  public List<TableDescriptor> getTables() {
    return Collections.unmodifiableList(this.tables);
  }
}