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

package org.apache.samza.util.hadoop

import java.io.IOException
import java.io.InputStream

import org.apache.hadoop.fs.FSInputStream

class HttpInputStream(is: InputStream) extends FSInputStream {
  val lock: AnyRef = new Object
  var pos: Long = 0

  override def seek(pos: Long) = throw new IOException("Seek not supported");

  override def getPos: Long = pos

  override def seekToNewSource(targetPos: Long): Boolean = throw new IOException("Seek not supported");

  override def read: Int = {
    lock.synchronized {
      var byteRead = is.read()
      if (byteRead >= 0) {
        pos += 1
      }
      byteRead
    }
  }
}
