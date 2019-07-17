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
package org.apache.samza.system.hdfs.descriptors;

import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.descriptors.OutputDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;

/**
 * A {@link HdfsOutputDescriptor} can be used for specifying Samza and HDFS-specific properties of HDFS
 * output streams.
 * <p>
 * Use {@link HdfsSystemDescriptor#getOutputDescriptor} to obtain an instance of this descriptor.
 * <p>
 * Stream properties provided in configuration override corresponding properties specified using a descriptor.
 */
public class HdfsOutputDescriptor
    extends OutputDescriptor<Object, HdfsOutputDescriptor> {

  /**
   * Constructs an {@link OutputDescriptor} instance. Hdfs output has no key. Value type is determined by
   * writer class (see {@link HdfsSystemDescriptor#withWriterClassName}).
   *
   * @param streamId id of the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   */
  HdfsOutputDescriptor(String streamId, SystemDescriptor systemDescriptor) {
    super(streamId, new NoOpSerde(), systemDescriptor);
  }
}
