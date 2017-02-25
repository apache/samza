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

import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemStreamPartition;


public class HdfsReaderFactory {
  public static SingleFileHdfsReader getHdfsReader(ReaderType readerType, SystemStreamPartition systemStreamPartition) {
    switch (readerType) {
      case AVRO: return new AvroFileHdfsReader(systemStreamPartition);
      default:
        throw new SamzaException("Unsupported reader type: " + readerType);
    }
  }

  public static ReaderType getType(String readerTypeStr) {
    try {
      return ReaderType.valueOf(readerTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new SamzaException("Invalid hdfs reader type string: " + readerTypeStr, e);
    }
  }

  public static int offsetComparator(ReaderType readerType, String offset1, String offset2) {
    switch (readerType) {
      case AVRO: return AvroFileHdfsReader.offsetComparator(offset1, offset2);
      default:
        throw new SamzaException("Unsupported reader type: " + readerType);
    }
  }

  /*
   * Support AVRO only so far. Implement <code>SingleFileHdfsReader</code> to support a variety of
   * file parsers. Can easily support "plain" text in the future (each line of the
   * text representing a record for example)
   */
  public enum ReaderType {
    AVRO
  }
}
