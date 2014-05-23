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

import org.junit.Test
import org.junit.Assert._
import org.apache.samza.system.SystemStreamPartition
import org.junit.AfterClass
import java.io.PrintWriter
import java.io.File
import org.apache.samza.Partition
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import org.junit.BeforeClass
import java.io.FileWriter

object TestFileReaderSystemConsumer {
  val consumer = new FileReaderSystemConsumer("file-reader", null)
  val files = List("empty.txt", "noEnter.txt", "oneEnter.txt", "twoEnter.txt", "moreEnter.txt")
  val ssp1 = new SystemStreamPartition("file-reader", files(0), new Partition(0))
  val ssp2 = new SystemStreamPartition("file-reader", files(1), new Partition(0))
  val ssp3 = new SystemStreamPartition("file-reader", files(2), new Partition(0))
  val ssp4 = new SystemStreamPartition("file-reader", files(3), new Partition(0))
  val ssp5 = new SystemStreamPartition("file-reader", files(4), new Partition(0))

  @BeforeClass
  def beforeCreateFiles {
    files.foreach(file => {
      val writer = new PrintWriter(new File(file))
      file match {
        case "empty.txt" =>
        case "noEnter.txt" => writer.write("first line")
        case "oneEnter.txt" => writer.write("first line \nsecond line")
        case "twoEnter.txt" => writer.write("first line \nsecond line \nother lines")
        case "moreEnter.txt" => writer.write("first line \nsecond line \nthird line \nother lines \n")
      }
      writer.close
    })
  }

  @AfterClass
  def afterDeleteFiles {
    files.foreach(file => (new File(file)).delete)
  }

  def appendFile {
    val fileWriter = new FileWriter("moreEnter.txt", true);
    fileWriter.write("This is a new line\n");
    fileWriter.close
  }
}

class TestFileReaderSystemConsumer {
  import TestFileReaderSystemConsumer._

  @Test
  def testRegisterAndPutCorrectMessagesOffsetsToBlockingQueue {
    consumer.register(ssp1, "0")
    consumer.register(ssp2, "0")
    consumer.register(ssp3, "0")
    consumer.register(ssp4, "12")
    consumer.register(ssp5, "25")

    // test register correctly
    assertEquals("0", consumer.systemStreamPartitionAndStartingOffset.getOrElse(ssp1, null))
    assertEquals("0", consumer.systemStreamPartitionAndStartingOffset.getOrElse(ssp2, null))
    assertEquals("0", consumer.systemStreamPartitionAndStartingOffset.getOrElse(ssp3, null))
    assertEquals("12", consumer.systemStreamPartitionAndStartingOffset.getOrElse(ssp4, null))
    assertEquals("25", consumer.systemStreamPartitionAndStartingOffset.getOrElse(ssp5, null))

    consumer.start
    Thread.sleep(500)

    val number: Integer = 1000
    val ssp1Number: java.util.Map[SystemStreamPartition, Integer] = HashMap(ssp1 -> number)
    val ssp2Number: java.util.Map[SystemStreamPartition, Integer] = HashMap(ssp2 -> number)
    val ssp3Number: java.util.Map[SystemStreamPartition, Integer] = HashMap(ssp3 -> number)
    val ssp4Number: java.util.Map[SystemStreamPartition, Integer] = HashMap(ssp4 -> number)
    val ssp5Number: java.util.Map[SystemStreamPartition, Integer] = HashMap(ssp5 -> number)

    val ssp1Result = consumer.poll(ssp1Number, 1000)
    val ssp2Result = consumer.poll(ssp2Number, 1000)
    val ssp3Result = consumer.poll(ssp3Number, 1000)
    val ssp4Result = consumer.poll(ssp4Number, 1000)

    assertEquals(0, ssp1Result.size)
    assertEquals(0, ssp2Result.size)

    assertEquals(1, ssp3Result.size)
    assertEquals("first line ", ssp3Result(0).getMessage)
    assertEquals("0", ssp3Result(0).getOffset)

    assertEquals(1, ssp4Result.size)
    assertEquals("second line ", ssp4Result(0).getMessage)
    assertEquals("12", ssp4Result(0).getOffset)

    appendFile
    Thread.sleep(1000)

    // ssp5 should read the new lines
    val ssp5Result = consumer.poll(ssp5Number, 1000)
    assertEquals(3, ssp5Result.size)
    assertEquals("This is a new line", ssp5Result(2).getMessage)
    assertEquals("50", ssp5Result(2).getOffset)
    assertEquals("other lines ", ssp5Result(1).getMessage)
    assertEquals("37", ssp5Result(1).getOffset)

    consumer.stop
  }
}
