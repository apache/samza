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

import java.util.List;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.TableDescriptor;


/**
 * Factory to create a list of {@link TableDescriptor} objects to describe one or more Samza tables. Developers writing
 * Samza jobs using Samza table(s) should describe the table(s) by implementing TableDescriptorFactory in their task.
 * Please note that the class that implements this factory should also implement one of
 * {@link org.apache.samza.task.StreamTask} or {@link org.apache.samza.task.AsyncStreamTask}.
 *
 * NOTE: {@link TableDescriptorsFactory} is instantiated in the config rewriter to get the table descriptors. Hence,
 * please DO NOT access any task instance member variables in the getTableDescriptors API.
 *
 * <p>
 * Typical user code using Samza tables should look like the following:
 *
 * <pre>
 * {@code
 * public class SampleTask implements TableDescriptorsFactory, InitableTask, StreamTask {
 *   private ReadableTable<String, Long> remoteTable;
 *   private ReadWriteTable<String, String> localTable;
 *
 *   @Override
 *   public List<TableDescriptor> getTableDescriptors() {
 *     List<TableDescriptor> tableDescriptors = new ArrayList<>();
 *     final TableReadFunction readRemoteTable = (TableReadFunction) key -> null;
 *     tableDescriptors.add(new RemoteTableDescriptor<>("remote-table-1")
 *       .withReadFunction(readRemoteTable)
 *       .withSerde(KVSerde.of(new StringSerde(), new StringSerde())));
 *
 *     tableDescriptors.add(new RocksDbTableDescriptor("local-table-1")
 *       .withBlockSize(4096)
 *       .withSerde(KVSerde.of(new LongSerde(), new StringSerde<>())));
 *       .withConfig("some-key", "some-value");
 *     return tableDescriptors;
 *   }
 *
 *   @Override
 *   public void init(Config config, TaskContext context) {
 *     remoteTable = (ReadableTable<String, String>) context.getTable(“remote-table-1”);
 *     localTable = (ReadWriteTable<Long, String>) context.getTable(“local-table-1”);
 *   }
 *
 *   @Override
 *   void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
 *      throws Exception {
 *     ..
 *     GenericRecord record = (GenericRecord) envelope.getMessage();
 *     Long memberId = (Long) record.get("memberId");
 *     String memberName = (String) localTable.get(memberId);
 *     // If the local table does not contain memberId, fetch the corresponding memberName from remote table and cache
 *     // it in the local table.
 *     if (company == null) {
 *       localTable.put(memberId, remoteTable.get(memberId));
 *     } else {
 *       // other logic goes here.
 *     }
 *     ..
 *   }
 * }
 * }
 * </pre>
 *
 * For the TableDescriptorsFactory to be picked up by the table config rewriter, please add the below properties to the
 * job config:
 *
 * <pre>
 * {@code
 *  <property name="job.config.rewriters" value="tableConfigRewriter"/>
 *  <property name="job.config.rewriter.tableConfigRewriter.class" value="org.apache.samza.config.TableConfigRewriter"/>
 * }
 * </pre>
 *
 * </p>
 */
@InterfaceStability.Unstable
public interface TableDescriptorsFactory {
  /**
   * Called by TableConfigRewriter to generate table configs.
   * Constructs instances of the table descriptors
   * @return list of table descriptors
   */
  List<TableDescriptor> getTableDescriptors();
}
