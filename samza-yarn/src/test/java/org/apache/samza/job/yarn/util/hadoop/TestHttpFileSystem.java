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

package org.apache.samza.job.yarn.util.hadoop;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.util.hadoop.HttpFileSystem;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;

/**
 * Test the behavior of {@link HttpFileSystem}
 */
public class TestHttpFileSystem {

  private static final Logger LOG = LoggerFactory.getLogger(TestHttpFileSystem.class);
  /**
   * Number of bytes the server should stream before hanging the TCP connection.
   */
  private static final int THRESHOLD_BYTES = 5;
  private static final String RESPONSE_STR = "HELLO WORLD";

  private final CountDownLatch serverWaitLatch = new CountDownLatch(1);

  private Exception clientException;
  private Exception serverException;

  /**
   * A {@link HttpServlet} implementation that streams its response to the client one byte at a time.
   */
  private class PartialFileFetchServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) {

      //Mimic download of the package tar-ball
      response.setContentType("application/gzip");
      response.setStatus(HttpServletResponse.SC_OK);

      try {
        int currByteWritten = -1;
        int numBytesWritten = 0;
        //Begin streaming a response.
        InputStream in = new ByteArrayInputStream(RESPONSE_STR.getBytes());
        OutputStream out = response.getOutputStream();

        while ((currByteWritten = in.read()) != -1) {
          out.write(currByteWritten);
          out.flush();
          numBytesWritten++;

          //Hang the connection until the read timeout expires on the client side.
          if (numBytesWritten >= THRESHOLD_BYTES) {
            serverWaitLatch.await();
          }
        }
      } catch(Exception e) {
        //Record any exception that may have occurred
        LOG.error("{}", e);
        serverException = e;
      }
    }
  }

  class FileSystemClientThread extends Thread {

    private static final int TIMEOUT_MS = 1000;
    private final URI resourceURI;
    private int totalBytesRead = 0;

    FileSystemClientThread(URI resourceURI) {
      this.resourceURI = resourceURI;
    }

    public int getTotalBytesRead() {
      return totalBytesRead;
    }

    @Override
    public void run() {
      try {
        Path resource = new Path(resourceURI);
        Configuration conf = new Configuration();
        HttpFileSystem fs = new HttpFileSystem();
        fs.setSocketReadTimeoutMs(TIMEOUT_MS);
        fs.setConnectionTimeoutMs(TIMEOUT_MS);
        fs.setConf(conf);
        fs.initialize(resourceURI, conf);

        //Read from the socket one byte at a time.
        FSDataInputStream in = fs.open(resource);
        while (in.read() >= 0) {
          totalBytesRead++;
        }
      } catch(SocketTimeoutException e) {
        //Expect the socket to timeout after THRESHOLD bytes have been read.
        serverWaitLatch.countDown();
      } catch(Exception e) {
        //Record any exception that may have occurred.
        LOG.error("{}", e);
        clientException = e;
      }
    }
  }

  @Test
  public void testHttpFileSystemReadTimeouts() throws Exception {
    HttpServer server = new HttpServer("/", 0, null, new ServletHolder(DefaultServlet.class));
    try {
      server.addServlet("/download", new PartialFileFetchServlet());
      server.start();
      String serverUrl = server.getUrl().toString() + "download";
      FileSystemClientThread fileSystemClientThread = new FileSystemClientThread(new URI(serverUrl));
      fileSystemClientThread.start();
      fileSystemClientThread.join();
      Assert.assertEquals(fileSystemClientThread.getTotalBytesRead(), THRESHOLD_BYTES);
      Assert.assertNull(clientException);
      Assert.assertNull(serverException);
    } finally {
      server.stop();
    }
  }
}

