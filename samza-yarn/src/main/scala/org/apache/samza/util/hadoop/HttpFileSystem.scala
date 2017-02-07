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
import java.net.URI

import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.HttpStatus
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Progressable

import org.apache.samza.util.Logging

class HttpFileSystem extends FileSystem with Logging {
  val DEFAULT_BLOCK_SIZE = 4 * 1024
  var uri: URI = null
  var connectionTimeoutMs = 5 * 60 * 1000
  var socketReadTimeoutMs = 5 * 60 * 1000

  def setConnectionTimeoutMs(timeout: Int): Unit = connectionTimeoutMs = timeout

  def setSocketReadTimeoutMs(timeout: Int): Unit = socketReadTimeoutMs = timeout

  override def initialize(uri: URI, conf: Configuration) {
    super.initialize(uri, conf)
    info("init uri %s" format (uri))
    this.uri = uri
  }

  override def getUri = uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    info("open http file %s" format (f))
    val client = new HttpClient
    client.getHttpConnectionManager.getParams.setConnectionTimeout(connectionTimeoutMs)
    client.getHttpConnectionManager.getParams.setSoTimeout(socketReadTimeoutMs)

    val method = new GetMethod(f.toUri.toString)
    val statusCode = client.executeMethod(method)

    if (statusCode != HttpStatus.SC_OK) {
      warn("got status code %d for uri %s" format (statusCode, uri))
      throw new IOException("Bad status code returned by http server " + f + ": " + statusCode)
    }

    new FSDataInputStream(new HttpInputStream(method.getResponseBodyAsStream))
  }

  override def create(f: Path,
    permission: FsPermission,
    overwrite: Boolean,
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: Progressable): FSDataOutputStream = null

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = null

  override def rename(src: Path, dst: Path): Boolean = false

  override def delete(f: Path, recursive: Boolean): Boolean = false

  override def listStatus(f: Path): Array[FileStatus] = null

  override def setWorkingDirectory(newDir: Path) {}

  override def getWorkingDirectory(): Path = new Path("/")

  override def mkdirs(f: Path, permission: FsPermission): Boolean = false

  override def getFileStatus(f: Path): FileStatus = {
    val length = -1
    val isDir = false
    val blockReplication = 1
    val blockSize = DEFAULT_BLOCK_SIZE
    val modTime = 0
    val fs = new FileStatus(length, isDir, blockReplication, blockSize, modTime, f)
    debug("file status for %s is %s" format (f, fs))
    return fs
  }
}

