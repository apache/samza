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

import org.apache.samza.system.IncomingMessageEnvelope;


public interface SingleFileHdfsReader {
  /**
   * Open the file and seek to specific offset for reading.
   * @param path path of the file to be read
   * @param offset offset the reader should start from
   */
  public void open(String path, String offset);

  /**
   * Seek to a specific offset
   * @param offset offset the reader should seek to
   */
  public void seek(String offset);

  /**
   * Construct and return the next message envelope
   * @return constructed IncomeMessageEnvelope
   */
  public IncomingMessageEnvelope readNext();

  /**
   * Get the next offset, which is the offset for the next message
   * that will be returned by readNext
   * @return next offset
   */
  public String nextOffset();

  /**
   * Whether there are still records to be read
   * @return true of false based on whether the reader has hit end of file
   */
  public boolean hasNext();

  /**
   * Close the reader.
   */
  public void close();
}
