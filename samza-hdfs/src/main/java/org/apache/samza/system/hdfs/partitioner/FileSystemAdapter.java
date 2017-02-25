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

package org.apache.samza.system.hdfs.partitioner;

import java.util.List;


/**
 * An adapter between directory partitioner and the actual file systems or
 * file system like systems.
 */
public interface FileSystemAdapter {

  /**
   * Return the list of all files given the stream name
   * @param streamName name of the stream
   * @return list of <code>FileMetadata</code> for all files associated to the given stream
   */
  public List<FileMetadata> getAllFiles(String streamName);

  public class FileMetadata {
    private String path;
    private long length;

    public FileMetadata(String path, long length) {
      this.path = path;
      this.length = length;
    }

    public String getPath() {
      return path;
    }

    public long getLen() {
      return length;
    }

    @Override
    public String toString() {
      return String.format("[path = %s, length = %s]", path, length);
    }
  }
}
