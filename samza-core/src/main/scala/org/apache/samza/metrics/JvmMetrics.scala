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

package org.apache.samza.metrics

import java.lang.management.ManagementFactory
import scala.collection._
import scala.collection.JavaConversions._
import java.lang.Thread.State._
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.samza.util.Logging
import org.apache.samza.util.DaemonThreadFactory

/**
 *  Companion object for class JvmMetrics encapsulating various constants
 */
object JvmMetrics {
  val JVM_METRICS_THREAD_NAME_PREFIX = "JVM-METRICS"
}

/**
 * Straight up ripoff of Hadoop's metrics2 JvmMetrics class.
 */
class JvmMetrics(val registry: MetricsRegistry) extends MetricsHelper with Runnable with Logging {
  final val M = 1024 * 1024.0F

  val memoryMXBean = ManagementFactory.getMemoryMXBean()
  val gcBeans = ManagementFactory.getGarbageCollectorMXBeans()
  val threadMXBean = ManagementFactory.getThreadMXBean()
  var gcBeanCounters = Map[String, (Counter, Counter)]()
  val executor = Executors.newScheduledThreadPool(1, new DaemonThreadFactory(JvmMetrics.JVM_METRICS_THREAD_NAME_PREFIX))

  // jvm metrics
  val gMemNonHeapUsedM = newGauge("mem-non-heap-used-mb", 0.0F)
  val gMemNonHeapCommittedM = newGauge("mem-non-heap-committed-mb", 0.0F)
  val gMemNonHeapMaxM = newGauge("mem-non-heap-max-mb", 0.0F)
  val gMemHeapUsedM = newGauge("mem-heap-used-mb", 0.0F)
  val gMemHeapCommittedM = newGauge("mem-heap-committed-mb", 0.0F)
  val gMemHeapMaxM = newGauge("mem-heap-max-mb", 0.0F)
  val gThreadsNew = newGauge("threads-new", 0L)
  val gThreadsRunnable = newGauge("threads-runnable", 0L)
  val gThreadsBlocked = newGauge("threads-blocked", 0L)
  val gThreadsWaiting = newGauge("threads-waiting", 0L)
  val gThreadsTimedWaiting = newGauge("threads-timed-waiting", 0L)
  val gThreadsTerminated = newGauge("threads-terminated", 0L)
  val cGcCount = newCounter("gc-count")
  val cGcTimeMillis = newCounter("gc-time-millis")

  def start {
    executor.scheduleWithFixedDelay(this, 0, 5, TimeUnit.SECONDS)
  }

  def run {
    debug("updating jvm metrics")

    updateMemoryUsage
    updateGcUsage
    updateThreadUsage

    debug("updated metrics to: [%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s]" format
      (gMemNonHeapUsedM, gMemNonHeapCommittedM, gMemNonHeapMaxM, gMemHeapUsedM, gMemHeapCommittedM,gMemHeapMaxM, gThreadsNew,
        gThreadsRunnable, gThreadsBlocked, gThreadsWaiting, gThreadsTimedWaiting,
        gThreadsTerminated, cGcCount, cGcTimeMillis))
  }

  def stop = executor.shutdown

  private def updateMemoryUsage {
    val memNonHeap = memoryMXBean.getNonHeapMemoryUsage()
    val memHeap = memoryMXBean.getHeapMemoryUsage()
    gMemNonHeapUsedM.set(memNonHeap.getUsed() / M)
    gMemNonHeapCommittedM.set(memNonHeap.getCommitted() / M)
    gMemNonHeapMaxM.set(memNonHeap.getMax / M)
    gMemHeapUsedM.set(memHeap.getUsed() / M)
    gMemHeapCommittedM.set(memHeap.getCommitted() / M)
    gMemHeapMaxM.set(memHeap.getMax() / M)
  }

  private def updateGcUsage {
    var count = 0l
    var timeMillis = 0l

    gcBeans.foreach(gcBean => {
      val c = gcBean.getCollectionCount()
      val t = gcBean.getCollectionTime()
      val gcInfo = getGcInfo(gcBean.getName)
      gcInfo._1.inc(c - gcInfo._1.getCount())
      gcInfo._2.inc(t - gcInfo._2.getCount())
      count += c
      timeMillis += t
    })

    cGcCount.inc(count - cGcCount.getCount())
    cGcTimeMillis.inc(timeMillis - cGcTimeMillis.getCount())
  }

  private def getGcInfo(gcName: String): (Counter, Counter) = {
    gcBeanCounters.get(gcName) match {
      case Some(gcBeanCounterTuple) => gcBeanCounterTuple
      case _ => {
        val t = (newCounter("%s-gc-count" format gcName), newCounter("%s-gc-time-millis" format gcName))
        gcBeanCounters += (gcName -> t)
        t
      }
    }
  }

  private def updateThreadUsage {
    var threadsNew = 0l
    var threadsRunnable = 0l
    var threadsBlocked = 0l
    var threadsWaiting = 0l
    var threadsTimedWaiting = 0l
    var threadsTerminated = 0l
    var threadIds = threadMXBean.getAllThreadIds

    threadMXBean.getThreadInfo(threadIds, 0).foreach(threadInfo =>
      Option(threadInfo) match {
        case Some(threadInfo) => {
          threadInfo.getThreadState match {
            case NEW => threadsNew += 1
            case RUNNABLE => threadsRunnable += 1
            case BLOCKED => threadsBlocked += 1
            case WAITING => threadsWaiting += 1
            case TIMED_WAITING => threadsTimedWaiting += 1
            case TERMINATED => threadsTerminated += 1
          }
        }
        case _ => // race protection
      })

    gThreadsNew.set(threadsNew)
    gThreadsRunnable.set(threadsRunnable)
    gThreadsBlocked.set(threadsBlocked)
    gThreadsWaiting.set(threadsWaiting)
    gThreadsTimedWaiting.set(threadsTimedWaiting)
    gThreadsTerminated.set(threadsTerminated)
  }
}
