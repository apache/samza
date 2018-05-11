/*
 *
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
 *
 */

package org.apache.samza.util

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}

import org.apache.samza.SamzaException
import org.apache.samza.util.Util.{error, warn}

object HttpUtil {

  /**
    * Reads a URL and returns the response body as a string. Retries in an exponential backoff, but does no other error handling.
    *
    * @param url HTTP URL to read from.
    * @param timeout how long to wait before timing out when connecting to or reading from the HTTP server.
    * @param retryBackoff instance of exponentialSleepStrategy that encapsulates info on how long to sleep and retry operation
    * @return string payload of the body of the HTTP response.
    */
  def read(url: URL, timeout: Int = 60000, retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy): String = {
    var httpConn = getHttpConnection(url, timeout)
    retryBackoff.run(loop => {
      if(httpConn.getResponseCode != 200)
      {
        warn("Error: " + httpConn.getResponseCode)
        val errorContent = readStream(httpConn.getErrorStream)
        warn("Error reading stream, failed with response %s" format errorContent)
        httpConn = getHttpConnection(url, timeout)
      }
      else
      {
        loop.done
      }
    },
      (exception, loop) => {
        exception match {
          case ioe: IOException => {
            warn("Error getting response from Job coordinator server. received IOException: %s. Retrying..." format ioe.getClass)
            httpConn = getHttpConnection(url, timeout)
          }
          case e: Exception =>
            loop.done
            error("Unable to connect to Job coordinator server, received exception", e)
            throw e
        }
      })

    if(httpConn.getResponseCode != 200) {
      throw new SamzaException("Unable to read JobModel from Jobcoordinator HTTP server")
    }
    readStream(httpConn.getInputStream)
  }

  def getHttpConnection(url: URL, timeout: Int): HttpURLConnection = {
    val conn = url.openConnection()
    conn.setConnectTimeout(timeout)
    conn.setReadTimeout(timeout)
    conn.asInstanceOf[HttpURLConnection]
  }

  private def readStream(stream: InputStream): String = {
    val br = new BufferedReader(new InputStreamReader(stream))
    var line: String = null
    val body = Iterator.continually(br.readLine()).takeWhile(_ != null).mkString
    br.close
    stream.close
    body
  }
}
