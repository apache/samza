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

package org.apache.samza.util.hadoop;

import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.mortbay.jetty.HttpParser;


public class CertFSInputStream extends FSInputStream {
  long _pos = 0;
  InputStream _is;

  public CertFSInputStream(InputStream is) {
    _is = is;
  }
  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't
   * seek past the end of the file.
   */
  @Override
  public void seek(long pos) throws IOException {
    throw new IOException("seek is not supported!");
  }

  /**
   * Return the current offset from the start of the file
   */
  @Override
  public long getPos() throws IOException {
    return _pos;
  }

  /**
   * Seeks a different copy of the data.  Returns true if
   * found a new source, false otherwise.
   */
  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("seek is not supported!");
  }

  @Override
  public int read() throws IOException {
    int byteRead = _is.read();
    if (byteRead >=0) {
      _pos++;
    }
    return byteRead;
  }

}
