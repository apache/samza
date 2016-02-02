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

package org.apache.samza.system.hdfs


import java.io.{File, IOException}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.hadoop.hdfs.{DFSConfigKeys,MiniDFSCluster}
import org.apache.hadoop.io.{SequenceFile, BytesWritable, LongWritable, Text}
import org.apache.hadoop.io.SequenceFile.Reader

import org.apache.samza.config.Config
import org.apache.samza.system.hdfs.HdfsConfig._
import org.apache.samza.config.factories.PropertiesConfigFactory
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.system.{OutgoingMessageEnvelope, SystemStream}
import org.apache.samza.util.Logging

import org.junit.{AfterClass, BeforeClass, Test}
import org.junit.Assert._

import scala.collection.JavaConverters._


object TestHdfsSystemProducerTestSuite {
  val TEST = "test"
  val BLOCK_SIZE = 128 * 1024 * 1024 // 128MB
  val BATCH_SIZE = 512 // in bytes, to get multiple file splits in one of the tests
  val PAUSE = 500 // in millis
  val JOB_NAME = "samza-hdfs-test-job" // write some data as BytesWritable
  val BATCH_JOB_NAME = "samza-hdfs-test-batch-job" // write enough binary data to force the producer to split partfiles
  val TEXT_JOB_NAME = "samza-hdfs-test-job-text" // write some data as String
  val TEXT_BATCH_JOB_NAME = "samza-hdfs-test-batch-job-text" // force a file split, understanding that Text does some compressing
  val RESOURCE_PATH_FORMAT = "file://%s/src/test/resources/%s.properties"
  val TEST_DATE = (new SimpleDateFormat("yyyy_MM_dd-HH")).format(new Date)

  // Test data
  val EXPECTED = Array[String]("small_data", "medium_data", "large_data")
  val LUMP = new scala.util.Random().nextString(BATCH_SIZE)

  val hdfsFactory = new TestHdfsSystemFactory()
  val propsFactory = new PropertiesConfigFactory()
  val cluster = getMiniCluster

  def testWritePath(job: String): Path = new Path(
    Seq("/user/", System.getProperty("user.name"), job, TEST_DATE).mkString("/")
  )

  def getMiniCluster: Option[MiniDFSCluster] = {
    val conf = new Configuration(false)
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE)

    val cluster = Some(new MiniDFSCluster.Builder(conf).numDataNodes(3).build)
    cluster.get.waitActive

    cluster
  }

  def buildProducer(name: String, cluster: MiniDFSCluster): Option[HdfsSystemProducer] @unchecked = {
   Some(
      hdfsFactory.getProducer(
        name,
        propsFactory.getConfig(URI.create(RESOURCE_PATH_FORMAT format (new File(".").getCanonicalPath, name))),
        new HdfsSystemProducerMetrics(name),
        cluster
      )
    )
  }

  def getReader(dfs: FileSystem, path: Path): SequenceFile.Reader = {
    new SequenceFile.Reader(dfs.getConf, Reader.file(path))
  }

  @AfterClass
  def tearDownMiniCluster: Unit = cluster.map{ _.shutdown }

}


class TestHdfsSystemProducerTestSuite extends Logging {
  import org.apache.samza.system.hdfs.TestHdfsSystemProducerTestSuite._

  @Test
  def testHdfsSystemProducerBinaryWrite {
    var producer: Option[HdfsSystemProducer] = None

    try {
      producer = buildProducer(JOB_NAME, cluster.get)
      producer.get.register(TEST)
      producer.get.start

      Thread.sleep(PAUSE)

      val systemStream = new SystemStream(JOB_NAME, TEST)
      EXPECTED.map { _.getBytes("UTF-8") }.map {
        buffer: Array[Byte] => producer.get.send(TEST, new OutgoingMessageEnvelope(systemStream, buffer))
      }

      producer.get.stop
      producer = None

      val results = cluster.get.getFileSystem.listStatus(testWritePath(JOB_NAME))
      val bytesWritten = results.toList.foldLeft(0L) { (acc, status) => acc + status.getLen }
      assertTrue(results.length == 1)
      assertTrue(bytesWritten > 0L)

      results.foreach { r =>
        val reader = getReader(cluster.get.getFileSystem, r.getPath)
        val key = new BytesWritable
        val value = new BytesWritable
        (0 to 2).foreach { i =>
          reader.next(key, value)
          assertEquals(EXPECTED(i), new String(value.copyBytes, "UTF-8"))
        }
      }
    } finally {
      producer.map { _.stop }
    }
  }

  @Test
  def testHdfsSystemProducerWriteBinaryBatches {
    var producer: Option[HdfsSystemProducer] = None

    try {
      producer = buildProducer(BATCH_JOB_NAME, cluster.get)

      producer.get.start
      producer.get.register(TEST)
      Thread.sleep(PAUSE)

      val systemStream = new SystemStream(BATCH_JOB_NAME, TEST)
      (1 to 6).map { i => LUMP.getBytes("UTF-8") }.map {
        buffer => producer.get.send(TEST, new OutgoingMessageEnvelope(systemStream, buffer))
      }

      producer.get.stop
      producer = None

      val results = cluster.get.getFileSystem.listStatus(testWritePath(BATCH_JOB_NAME))

      assertEquals(6, results.length)
      results.foreach { r =>
        val reader = getReader(cluster.get.getFileSystem, r.getPath)
        val key = new BytesWritable
        val value = new BytesWritable
        (1 to BATCH_SIZE).foreach { i =>
          reader.next(key, value)
          val data = value.copyBytes
          assertEquals(LUMP, new String(data, "UTF-8"))
          assertEquals(LUMP.getBytes("UTF-8").length, data.length)
        }
      }

    } finally {
      producer.map { _.stop }
    }
  }

  @Test
  def testHdfsSystemProducerTextWrite {
    var producer: Option[HdfsSystemProducer] = None

    try {
      producer = buildProducer(TEXT_JOB_NAME, cluster.get)
      producer.get.register(TEST)
      producer.get.start

      Thread.sleep(PAUSE)

      val systemStream = new SystemStream(TEXT_JOB_NAME, TEST)
      EXPECTED.map {
        buffer: String => producer.get.send(TEST, new OutgoingMessageEnvelope(systemStream, buffer))
      }

      producer.get.stop
      producer = None

      val results = cluster.get.getFileSystem.listStatus(testWritePath(TEXT_JOB_NAME))
      val bytesWritten = results.toList.foldLeft(0L) { (acc, status) => acc + status.getLen }
      assertTrue(results.length == 1)
      assertTrue(bytesWritten > 0L)

      results.foreach { r =>
        val reader = getReader(cluster.get.getFileSystem, r.getPath)
        val key = new LongWritable
        val value = new Text
        (0 to 2).foreach { i =>
          reader.next(key, value)
          assertEquals(EXPECTED(i), value.toString)
        }
      }
    } finally {
      producer.map { _.stop }
    }
  }

  @Test
  def testHdfsSystemProducerWriteTextBatches {
    var producer: Option[HdfsSystemProducer] = None

    try {
      producer = buildProducer(TEXT_BATCH_JOB_NAME, cluster.get)

      producer.get.start
      producer.get.register(TEST)
      Thread.sleep(PAUSE)

      val systemStream = new SystemStream(TEXT_BATCH_JOB_NAME, TEST)
      (1 to 6).map {
        i => producer.get.send(TEST, new OutgoingMessageEnvelope(systemStream, LUMP))
      }

      producer.get.stop
      producer = None

      val results = cluster.get.getFileSystem.listStatus(testWritePath(TEXT_BATCH_JOB_NAME))

      assertEquals(6, results.length)
      results.foreach { r =>
        val reader = getReader(cluster.get.getFileSystem, r.getPath)
        val key = new LongWritable
        val value = new Text
        (1 to BATCH_SIZE).foreach { i =>
          reader.next(key, value)
          val data = value.toString
          assertEquals(LUMP, data)
          assertEquals(LUMP.length, data.length)
        }
      }

    } finally {
      producer.map { _.stop }
    }
  }

}


class TestHdfsSystemProducer(systemName: String, config: HdfsConfig, clientId: String, metrics: HdfsSystemProducerMetrics, mini: MiniDFSCluster)
  extends HdfsSystemProducer(systemName, clientId, config, metrics) {
    override val dfs = mini.getFileSystem
}


class TestHdfsSystemFactory extends HdfsSystemFactory {
    def getProducer(systemName: String, config: Config, metrics: HdfsSystemProducerMetrics, cluster: MiniDFSCluster) = {
      new TestHdfsSystemProducer(systemName, config, "test", metrics, cluster)
    }
}
