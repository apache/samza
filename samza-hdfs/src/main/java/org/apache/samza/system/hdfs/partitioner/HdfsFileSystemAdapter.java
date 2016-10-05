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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HdfsFileSystemAdapter implements FileSystemAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(HdfsFileSystemAdapter.class);

  public List<FileMetadata> getAllFiles(String streamName) {
    List<FileMetadata> ret = new ArrayList<>();
    try {
      Path streamPath = new Path(streamName);
      FileSystem fileSystem = streamPath.getFileSystem(new Configuration());
      FileStatus[] fileStatuses = fileSystem.listStatus(streamPath);
      for (FileStatus fileStatus : fileStatuses) {
        ret.add(new FileMetadata(fileStatus.getPath().toString(), fileStatus.getLen()));
      }
    } catch (IOException e) {
      LOG.error("Failed to get the list of files for " + streamName, e);
      throw new SamzaException(e);
    }
    return ret;
  }
}
