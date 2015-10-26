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

package org.apache.samza.util

import java.net._
import java.io._
import java.lang.management.ManagementFactory
import java.util.zip.CRC32
import org.apache.samza.{SamzaException, Partition}
import org.apache.samza.system.{SystemFactory, SystemStreamPartition, SystemStream}
import java.util.Random
import org.apache.samza.config.Config
import org.apache.samza.config.SystemConfig
import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.ConfigException
import org.apache.samza.config.MapConfig
import scala.collection.JavaConversions._
import org.apache.samza.config.JobConfig
import java.io.InputStreamReader
import scala.collection.immutable.Map
import org.apache.samza.serializers._

object Util extends Logging {
  val random = new Random

  def clock: Long = System.currentTimeMillis
  /**
   * Make an environment variable string safe to pass.
   */
  def envVarEscape(str: String) = str.replace("\"", "\\\"").replace("'", "\\'")

  /**
   * Get a random number >= startInclusive, and < endExclusive.
   */
  def randomBetween(startInclusive: Int, endExclusive: Int) =
    startInclusive + random.nextInt(endExclusive - startInclusive)

  /**
   * Recursively remove a directory (or file), and all sub-directories. Equivalent
   * to rm -rf.
   */
  def rm(file: File) {
    if (file == null) {
      return
    } else if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) {
        for (f <- files)
          rm(f)
      }
      file.delete()
    } else {
      file.delete()
    }
  }

  /**
   * Instantiate a class instance from a given className.
   */
  def getObj[T](className: String) = {
    Class
      .forName(className)
      .newInstance
      .asInstanceOf[T]
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  def getSystemStreamFromNames(systemStreamNames: String): SystemStream = {
    val idx = systemStreamNames.indexOf('.')
    if (idx < 0) {
      throw new IllegalArgumentException("No '.' in stream name '" + systemStreamNames + "'. Stream names should be in the form 'system.stream'")
    }
    new SystemStream(systemStreamNames.substring(0, idx), systemStreamNames.substring(idx + 1, systemStreamNames.length))
  }

  /**
   * Returns a SystemStream object based on the system stream name given. For
   * example, kafka.topic would return new SystemStream("kafka", "topic").
   */
  def getNameFromSystemStream(systemStream: SystemStream) = {
    systemStream.getSystem + "." + systemStream.getStream
  }

  /**
   * Makes sure that an object is not null, and throws a NullPointerException
   * if it is.
   */
  def notNull[T](obj: T, msg: String) = if (obj == null) {
    throw new NullPointerException(msg)
  }

  /**
   * Returns the name representing the JVM. It usually contains the PID of the process plus some additional information
   * @return String that contains the name representing this JVM
   */
  def getContainerPID(): String = {
    ManagementFactory.getRuntimeMXBean().getName()
  }

  /**
   * Reads a URL and returns its body as a string. Does no error handling.
   *
   * @param url HTTP URL to read from.
   * @param timeout How long to wait before timing out when connecting to or reading from the HTTP server.
   * @return String payload of the body of the HTTP response.
   */
  def read(url: URL, timeout: Int = 60000): String = {
    var httpConn = getHttpConnection(url, timeout)
    val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy
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

  private def getHttpConnection(url: URL, timeout: Int): HttpURLConnection = {
    val conn = url.openConnection()
    conn.setConnectTimeout(timeout)
    conn.setReadTimeout(timeout)
    conn.asInstanceOf[HttpURLConnection]
  }
  private def readStream(stream: InputStream): String = {
    val br = new BufferedReader(new InputStreamReader(stream));
    var line: String = null;
    val body = Iterator.continually(br.readLine()).takeWhile(_ != null).mkString
    br.close
    stream.close
    body
  }

  /**
   * Generates a coordinator stream name based off of the job name and job id
   * for the jobd. The format is of the stream name will be
   * __samza_coordinator_&lt;JOBNAME&gt;_&lt;JOBID&gt;.
   */
  def getCoordinatorStreamName(jobName: String, jobId: String) = {
    "__samza_coordinator_%s_%s" format (jobName.replaceAll("_", "-"), jobId.replaceAll("_", "-"))
  }

  /**
   * Get a job's name and ID given a config. Job ID is defaulted to 1 if not
   * defined in the config, and job name must be defined in config.
   *
   * @return A tuple of (jobName, jobId)
   */
  def getJobNameAndId(config: Config) = {
    (config.getName.getOrElse(throw new ConfigException("Missing required config: job.name")), config.getJobId.getOrElse("1"))
  }

  /**
   * Given a job's full config object, build a subset config which includes
   * only the job name, job id, and system config for the coordinator stream.
   */
  def buildCoordinatorStreamConfig(config: Config) = {
    val (jobName, jobId) = getJobNameAndId(config)
    // Build a map with just the system config and job.name/job.id. This is what's required to start the JobCoordinator.
    new MapConfig(config.subset(SystemConfig.SYSTEM_PREFIX format config.getCoordinatorSystemName, false) ++
      Map[String, String](JobConfig.JOB_NAME -> jobName, JobConfig.JOB_ID -> jobId, JobConfig.JOB_COORDINATOR_SYSTEM -> config.getCoordinatorSystemName))
  }

  /**
   * Get the Coordinator System and system factory from the configuration
   * @param config
   * @return
   */
  def getCoordinatorSystemStreamAndFactory(config: Config) = {
    val systemName = config.getCoordinatorSystemName
    val (jobName, jobId) = Util.getJobNameAndId(config)
    val streamName = Util.getCoordinatorStreamName(jobName, jobId)
    val coordinatorSystemStream = new SystemStream(systemName, streamName)
    val systemFactoryClassName = config
      .getSystemFactory(systemName)
      .getOrElse(throw new SamzaException("Missing configuration: " + SystemConfig.SYSTEM_FACTORY format systemName))
    val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
    (coordinatorSystemStream, systemFactory)
  }

  /**
   * The helper function converts a SSP to a string
   * @param ssp System stream partition
   * @return The string representation of the SSP
   */
  def sspToString(ssp: SystemStreamPartition): String = {
     ssp.getSystem() + "." + ssp.getStream() + "." + String.valueOf(ssp.getPartition().getPartitionId())
  }

  /**
   * The method converts the string SSP back to a SSP
   * @param ssp The string form of the SSP
   * @return An SSP typed object
   */
  def stringToSsp(ssp: String): SystemStreamPartition = {
     val idx = ssp.indexOf('.');
     val lastIdx = ssp.lastIndexOf('.')
     if (idx < 0 || lastIdx < 0) {
       throw new IllegalArgumentException("System stream partition expected in format 'system.stream.partition")
     }
     new SystemStreamPartition(new SystemStream(ssp.substring(0, idx), ssp.substring(idx + 1, lastIdx)),
                               new Partition(Integer.parseInt(ssp.substring(lastIdx + 1))))
  }

  /**
   * Method to generate the CRC32 checksum code for any given data
   * @param data The string for which checksum has to be generated
   * @return long type value representing the checksum
   * */
  def getChecksumValue(data: String) = {
    val crc = new CRC32
    crc.update(data.getBytes)
    crc.getValue
  }

  /**
   * Method that always writes checksum & data to a file
   * Checksum is pre-fixed to the data and is a 32-bit long type data.
   * @param file The file handle to write to
   * @param data The data to be written to the file
   * */
  def writeDataToFile(file: File, data: String) = {
    val checksum = getChecksumValue(data)
    var oos: ObjectOutputStream = null
    var fos: FileOutputStream = null
    try {
      fos = new FileOutputStream(file)
      oos = new ObjectOutputStream(fos)
      oos.writeLong(checksum)
      oos.writeUTF(data)
    } finally {
      oos.close()
      fos.close()
    }
  }

  /**
   * Method to read from a file that has a checksum prepended to the data
   * @param file The file handle to read from
   * */
  def readDataFromFile(file: File) = {
    var fis: FileInputStream = null
    var ois: ObjectInputStream = null
    try {
      fis = new FileInputStream(file)
      ois = new ObjectInputStream(fis)
      val checksumFromFile = ois.readLong()
      val data = ois.readUTF()
      if(checksumFromFile == getChecksumValue(data)) {
        data
      } else {
        info("Checksum match failed. Data in file is corrupted. Skipping content.")
        null
      }
    } finally {
      ois.close()
      fis.close()
    }
  }

  /**
   * Convert a java map to a Scala map
   * */
  def javaMapAsScalaMap[T, K](javaMap: java.util.Map[T, K]): Map[T, K] = {
    javaMap.toMap
  }

  /**
   * Returns the the first host address which is not the loopback address, or {@link java.net.InetAddress#getLocalHost InetAddress.getLocalhost()} as a fallback
   *
   * @return the {@link java.net.InetAddress InetAddress} which represents the localhost
   */
  def getLocalHost: InetAddress = {
    val localHost = InetAddress.getLocalHost
    if (localHost.isLoopbackAddress) {
      warn("Hostname %s resolves to a loopback address, trying to resolve an external IP address.".format(localHost.getHostName))
      val networkInterfaces = if (System.getProperty("os.name").startsWith("Windows")) NetworkInterface.getNetworkInterfaces.toList else NetworkInterface.getNetworkInterfaces.toList.reverse
      for (networkInterface <- networkInterfaces) {
        val addresses = networkInterface.getInetAddresses.toList.filterNot(address => address.isLinkLocalAddress || address.isLoopbackAddress)
        if (addresses.nonEmpty) {
          val address = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
          debug("Found an external IP address %s which represents the localhost.".format(address.getHostAddress))
          return InetAddress.getByAddress(address.getAddress)
        }
      }
    }
    localHost
  }

  /**
   * A helper function which returns system's default serde factory class according to the
   * serde name. If not found, throw exception.
   */
  def defaultSerdeFactoryFromSerdeName(serdeName: String) = {
    info("looking for default serdes")

    val serde = serdeName match {
      case "byte" => classOf[ByteSerdeFactory].getCanonicalName
      case "bytebuffer" => classOf[ByteBufferSerdeFactory].getCanonicalName
      case "integer" => classOf[IntegerSerdeFactory].getCanonicalName
      case "json" => classOf[JsonSerdeFactory].getCanonicalName
      case "long" => classOf[LongSerdeFactory].getCanonicalName
      case "serializable" => classOf[SerializableSerdeFactory[java.io.Serializable]].getCanonicalName
      case "string" => classOf[StringSerdeFactory].getCanonicalName
      case _ => throw new SamzaException("No class defined for serde %s" format serdeName)
    }
    info("use default serde %s for %s" format (serde, serdeName))
    serde
  }
}
