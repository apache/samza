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


import org.apache.samza.config.JobConfig.Config2Job
import org.apache.samza.config._
import org.apache.samza.system.SystemStream
import org.apache.samza.SamzaException

import java.lang.management.ManagementFactory
import java.net.Inet4Address
import java.net.InetAddress
import java.net.NetworkInterface
import java.util.Random

import scala.collection.JavaConverters._


object Util extends Logging {
  val Random = new Random
  val ThreadMxBean = ManagementFactory.getThreadMXBean

  /**
   * Make an environment variable string safe to pass.
   */
  def envVarEscape(str: String) = str.replace("\"", "\\\"").replace("'", "\\'")

  /**
   * Get a random number >= startInclusive, and < endExclusive.
   */
  def randomBetween(startInclusive: Int, endExclusive: Int) =
    startInclusive + Random.nextInt(endExclusive - startInclusive)

  /**
   * Instantiate an object of type T from a given className.
   */
  def getObj[T](className: String, clazz: Class[T]) = {
    try {
      Class
        .forName(className)
        .newInstance
        .asInstanceOf[T]
    } catch {
      case e: Throwable => {
        error("Unable to create an instance for class %s." format className, e)
        throw e
      }
    }
  }

  /**
   * Returns the the first host address which is not the loopback address, or [[java.net.InetAddress#getLocalHost]] as a fallback
   *
   * @return the [[java.net.InetAddress]] which represents the localhost
   */
  def getLocalHost: InetAddress = {
    val localHost = InetAddress.getLocalHost
    if (localHost.isLoopbackAddress) {
      debug("Hostname %s resolves to a loopback address, trying to resolve an external IP address.".format(localHost.getHostName))
      val networkInterfaces = if (System.getProperty("os.name").startsWith("Windows")) {
        NetworkInterface.getNetworkInterfaces.asScala.toList
      } else {
        NetworkInterface.getNetworkInterfaces.asScala.toList.reverse
      }
      for (networkInterface <- networkInterfaces) {
        val addresses = networkInterface.getInetAddresses.asScala.toList
          .filterNot(address => address.isLinkLocalAddress || address.isLoopbackAddress)
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
   * Re-writes configuration using a ConfigRewriter, if one is defined. If
   * there is no ConfigRewriter defined for the job, then this method is a
   * no-op.
   *
   * @param config The config to re-write
   * @return re-written config
   */
  def rewriteConfig(config: Config): Config = {
    def rewrite(c: Config, rewriterName: String): Config = {
      val rewriterClassName = config
              .getConfigRewriterClass(rewriterName)
              .getOrElse(throw new SamzaException("Unable to find class config for config rewriter %s." format rewriterName))
      val rewriter = Util.getObj(rewriterClassName, classOf[ConfigRewriter])
      info("Re-writing config with " + rewriter)
      rewriter.rewrite(rewriterName, c)
    }

    config.getConfigRewriters match {
      case Some(rewriters) => rewriters.split(",").foldLeft(config)(rewrite(_, _))
      case _ => config
    }
  }

  def logThreadDump(message: String): Unit = {
    try {
      val threadInfo = ThreadMxBean.dumpAllThreads(true, true)
      val sb = new StringBuilder
      sb.append(message).append("\n")
      for (ti <- threadInfo) {
        sb.append(ti.toString).append("\n")
      }
      info(sb)
    } catch {
      case e: Exception =>
        info("Could not get and log a thread dump", e)
    }
  }
}
