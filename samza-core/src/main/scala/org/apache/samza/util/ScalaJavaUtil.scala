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

import scala.collection.immutable.Map
import scala.collection.JavaConverters._
import scala.runtime.AbstractFunction0

object ScalaJavaUtil {

  /**
    * Convert a Java map to a Scala immutable Map
    * */
  def toScalaMap[K, V](javaMap: java.util.Map[K, V]): Map[K, V] = {
    javaMap.asScala.toMap
  }

  /**
    * Wraps the provided value in an Scala Function, e.g. for use in [[Option#getOrDefault]]
    *
    * @param value the value to be wrapped
    * @tparam T type of the value
    * @return an AbstractFunction0 that returns contained value when called
    */
  def defaultValue[T](value: T): AbstractFunction0[T] = {
    new AbstractFunction0[T] {
      override def apply(): T = value
    }
  }

  /**
    * Wraps the provided Java Supplier in an Scala Function, e.g. for use in [[Option#getOrDefault]]
    *
    * @param javaFunction the java Supplier function to be wrapped
    * @tparam T type of the value
    * @return an AbstractFunction0 that returns contained value when called
    */
  def toScalaFunction[T](javaFunction: java.util.function.Supplier[T]): AbstractFunction0[T] = {
    new AbstractFunction0[T] {
      override def apply(): T = javaFunction.get()
    }
  }
}
