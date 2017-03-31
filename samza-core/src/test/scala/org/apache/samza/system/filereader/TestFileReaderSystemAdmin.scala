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

import java.io.PrintWriter
import java.io.File

import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import org.junit.After
import org.scalatest.junit.AssertionsForJUnit

import scala.collection.mutable.HashMap
import scala.collection.JavaConverters._

class TestFileReaderSystemAdmin extends AssertionsForJUnit {

  val files = List("empty.txt", "noEnter.txt", "oneEnter.txt", "twoEnter.txt", "moreEnter.txt")

  @Before
  def createFiles {
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

  @After
  def deleteFiles {
    files.foreach(file => (new File(file)).delete)
  }

  @Test
  def testGetOffsetsAfter {
    val fileReaderSystemAdmin = new FileReaderSystemAdmin
    val ssp1 = new SystemStreamPartition("file-reader", files(0), new Partition(0))
    val ssp2 = new SystemStreamPartition("file-reader", files(1), new Partition(0))
    val ssp3 = new SystemStreamPartition("file-reader", files(2), new Partition(0))
    val ssp4 = new SystemStreamPartition("file-reader", files(3), new Partition(0))
    val ssp5 = new SystemStreamPartition("file-reader", files(4), new Partition(0))

    val offsets: java.util.Map[SystemStreamPartition, String] =
      HashMap(ssp3 -> "0", ssp4 -> "12", ssp5 -> "25").asJava
    val afterOffsets = fileReaderSystemAdmin.getOffsetsAfter(offsets)
    assertEquals("12", afterOffsets.get(ssp3))
    assertEquals("25", afterOffsets.get(ssp4))
    assertEquals("37", afterOffsets.get(ssp5))
  }

  @Test
  def testGetSystemStreamMetadata {
    val fileReaderSystemAdmin = new FileReaderSystemAdmin
    val allMetadata = fileReaderSystemAdmin.getSystemStreamMetadata(setAsJavaSetConverter(files.toSet).asJava)
    val expectedEmpty = new SystemStreamPartitionMetadata(null, null, "0")
    val expectedNoEntry = new SystemStreamPartitionMetadata("0", "0", "0")
    val expectedOneEntry = new SystemStreamPartitionMetadata("0", "0", "12")
    val expectedTwoEntry = new SystemStreamPartitionMetadata("0", "12", "25")
    val expectedMoreEntry = new SystemStreamPartitionMetadata("0", "37", "50")

    allMetadata.asScala.foreach { entry =>
      {
        val result = (entry._2).getSystemStreamPartitionMetadata().get(new Partition(0))
        entry._1 match {
          case "empty.txt" => assertEquals(expectedEmpty, result)
          case "noEnter.txt" => assertEquals(expectedNoEntry, result)
          case "oneEnter.txt" => assertEquals(expectedOneEntry, result)
          case "twoEnter.txt" => assertEquals(expectedTwoEntry, result)
          case "moreEnter.txt" => assertEquals(expectedMoreEntry, result)
        }
      }
    }
  }
}
