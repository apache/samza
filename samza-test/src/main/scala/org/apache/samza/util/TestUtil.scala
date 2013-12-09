package org.apache.samza.util

import org.junit.Assert._

object TestUtil {
  def expect[T](exception: Class[T], msg: Option[String] = None)(block: => Unit) = try {
    block
  } catch {
    case e => if (msg.isDefined) {
      assertEquals(msg.get, e.getMessage)
    }
    case _ => fail("Expected an NPE.")
  }
}