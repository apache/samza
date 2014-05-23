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

package org.apache.samza.system.filereader

import org.apache.samza.system.SystemConsumer
import org.apache.samza.util.BlockingEnvelopeMap
import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.system.SystemStreamPartition
import scala.collection.mutable.Map
import java.io.RandomAccessFile
import org.apache.samza.system.IncomingMessageEnvelope
import java.util.concurrent.LinkedBlockingQueue
import org.apache.samza.Partition
import collection.JavaConversions._
import scala.collection.mutable.HashMap
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import org.apache.samza.util.DaemonThreadFactory
import org.apache.samza.SamzaException
import grizzled.slf4j.Logging

object FileReaderSystemConsumer {
  /**
   * prefix for the file reader system thread names
   */
  val FILE_READER_SYSTEM_THREAD_PREFIX = "filereader-"
}

class FileReaderSystemConsumer(
  systemName: String,
  metricsRegistry: MetricsRegistry,

  /**
   * Threshold used to determine when there are too many IncomingMessageEnvelopes to be put onto
   * the BlockingQueue.
   */
  queueSize: Int = 10000,

  /**
   * the sleep interval of checking the file length. Unit of the time is milliseconds.
   */
  pollingSleepMs: Int = 500) extends BlockingEnvelopeMap with Logging {

  /**
   * a map for storing a systemStreamPartition and its starting offset.
   */
  var systemStreamPartitionAndStartingOffset = Map[SystemStreamPartition, String]()

  /**
   * a thread pool for the threads reading files.
   * The size of the pool equals to the number of files to read.
   */
  var pool: ExecutorService = null

  /**
   * register the systemStreamPartition and put they SystemStreampartition and its starting offset
   * into the systemStreamPartitionAndStartingOffset map
   */
  override def register(systemStreamPartition: SystemStreamPartition, startingOffset: String) {
    super.register(systemStreamPartition, startingOffset)
    systemStreamPartitionAndStartingOffset += ((systemStreamPartition, startingOffset))
  }

  /**
   * start one thread for each file reader
   */
  override def start {
    pool = Executors.newFixedThreadPool(systemStreamPartitionAndStartingOffset.size, new DaemonThreadFactory(FileReaderSystemConsumer.FILE_READER_SYSTEM_THREAD_PREFIX))
    systemStreamPartitionAndStartingOffset.map { case (ssp, offset) => pool.execute(readInputFiles(ssp, offset)) }
  }

  /**
   * Stop all the running threads
   */
  override def stop {
    pool.shutdown
  }

  /**
   * The method returns a runnable object, which reads a file until reach the end of the file. It puts
   * every line (ends with \n) and its offset (the beginning of the line) into BlockingQueue. If a line
   * is not ended with \n, it is thought as uncompleted. Therefore the thread will wait until the line
   * is completed and then put it into queue. The thread keeps comparing the file length with file pointer
   * to read the latest/updated file content. If the file is read to the end of current content, setIsHead()
   * is called to specify that the SystemStreamPartition has "caught up". The thread sleep time between
   * two compares is determined by <code>pollingSleepMs</code>
   */
  private def readInputFiles(ssp: SystemStreamPartition, startingOffset: String) = {
    new Runnable {
      @volatile var shutdown = false // tag to indicate the thread should stop running

      def run() {
        val path = ssp.getStream
        var file: RandomAccessFile = null
        var filePointer = startingOffset.toLong
        var line = "" // used to form a line of characters
        var offset = filePointer // record the beginning offset of a line
        try {
          file = new RandomAccessFile(path, "r")
          while (!shutdown) {
            if (file.length <= filePointer) {
              Thread.sleep(pollingSleepMs)
              file.close
              file = new RandomAccessFile(path, "r")
            } else {
              file.seek(filePointer)
              var i = filePointer
              while (i < file.length) {
                val cha = file.read.toChar
                if (cha == '\n') {
                  // put into the queue. offset is the beginning of this line
                  put(ssp, new IncomingMessageEnvelope(ssp, offset.toString, null, line));
                  offset = i + 1 // the beginning of the newline
                  line = ""
                } else {
                  line = line + cha
                }
                i += 1
              }
              filePointer = file.length
              setIsAtHead(ssp, true)
            }
          }
        } catch {
          case ie: InterruptedException => {
            // Swallow this exception since we don't need to clutter the logs 
            // with interrupt exceptions when shutting down.
            info("Received an interrupt while reading file. Shutting down.")
          }
        } finally {
          if (file != null) {
            file.close
          }
        }
      }

      // stop the thread gracefully by changing the variable's value
      def stop() {
        shutdown = true
      }
    }
  }

  /**
   * Constructs a new bounded BlockingQueue of IncomingMessageEnvelopes. The bound is determined
   * by the <code>BOUNDED_QUEUE_THRESHOLD</code> constant.
   *
   * @return A bounded queue used for queueing IncomingMessageEnvelopes to be sent to their
   *         specified destinations.
   */
  override def newBlockingQueue = {
    new LinkedBlockingQueue[IncomingMessageEnvelope](queueSize);
  }
}