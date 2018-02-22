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

package org.apache.samza.system.hdfs.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * An implementation of the HdfsReader that reads and processes json format
 * files.
 */
public class TextFileHdfsReader implements SingleFileHdfsReader {

  private static final Logger LOG = LoggerFactory.getLogger(TextFileHdfsReader.class);

  private final SystemStreamPartition systemStreamPartition;
  private BufferedReader fileReader;
  private long curBlockStart;
  private long curRecordOffset;

  public TextFileHdfsReader(SystemStreamPartition systemStreamPartition) {
    this.systemStreamPartition = systemStreamPartition;
    this.fileReader = null;
  }

  @Override
  public void open(String pathStr, String singleFileOffset) {
    LOG.info(String.format("%s: Open file [%s] with file offset [%s] for read", systemStreamPartition, pathStr, singleFileOffset));
    Path path = new Path(pathStr);
    try {
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(path.toUri(), conf);
      InputStreamReader input = new InputStreamReader(fs.open(path));
      fileReader = new BufferedReader(input);
      seek(singleFileOffset);
    } catch (IOException e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public void seek(String offset) {
    ;
  }

  @Override
  public IncomingMessageEnvelope readNext() {
    try {
      return new IncomingMessageEnvelope(systemStreamPartition, null, null, fileReader.readLine().getBytes());
    } catch (IOException e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public String nextOffset() {
    return "0:0";
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public void close() {
    LOG.info("About to close file reader for " + systemStreamPartition);
    try {
      fileReader.close();
    } catch (IOException e) {
      throw new SamzaException(e);
    }
    LOG.info("File reader closed for " + systemStreamPartition);
  }

}
