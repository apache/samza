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
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;


/**
 * A {@link HdfsInputDescriptor} can be used for specifying Samza and HDFS specific properties of HDFS
 * input streams.
 * <p>
 * Use {@link HdfsSystemDescriptor#getInputDescriptor} to obtain an instance of this descriptor.
 * <p>
 * Stream properties provided in configuration override corresponding properties specified using a descriptor.
 *
 */
public class HdfsInputDescriptor
    extends InputDescriptor<Object, HdfsInputDescriptor> {

  /**
   * Constructs an {@link InputDescriptor} instance. Hdfs input has no key. Value type is determined by
   * reader type (see {@link HdfsSystemDescriptor#withReaderType}).
   *
   * @param streamId id of the stream
   * @param systemDescriptor system descriptor this stream descriptor was obtained from
   */
  HdfsInputDescriptor(String streamId, SystemDescriptor systemDescriptor) {
    super(streamId, new NoOpSerde(), systemDescriptor, null);
  }
}
