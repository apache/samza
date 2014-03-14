package org.apache.samza.system.kafka

import org.junit._
import org.junit.Assert._
import kafka.api._
import kafka.common.TopicAndPartition
import org.mockito.{ Matchers, Mockito }
import org.mockito.Mockito._
import org.mockito.Matchers._
import kafka.consumer.SimpleConsumer
import kafka.message.Message
import kafka.message.ByteBufferMessageSet
import java.nio.ByteBuffer

class TestGetOffset {

  /**
   * An empty message set is still a valid offset. It just means that the
   * offset was for the upcoming message, which hasn't yet been written. The
   * fetch request times out in such a case, and an empty message set is
   * returned.
   */
  @Test
  def testIsValidOffsetWorksWithEmptyMessageSet {
    val getOffset = new GetOffset(OffsetRequest.LargestTimeString)
    // Should not throw an exception.
    assertTrue(getOffset.isValidOffset(getMockDefaultFetchSimpleConsumer, TopicAndPartition("foo", 1), "1234"))
  }

  /**
   * Create a default fetch simple consumer that returns empty message sets.
   */
  def getMockDefaultFetchSimpleConsumer = {
    new DefaultFetchSimpleConsumer("", 0, 0, 0, "") {
      val sc = Mockito.mock(classOf[SimpleConsumer])

      // Build an empty fetch response.
      val fetchResponse = {
        val fetchResponse = Mockito.mock(classOf[FetchResponse])
        val messageSet = {
          val messageSet = Mockito.mock(classOf[ByteBufferMessageSet])
          val messages = List()

          def getMessage() = new Message(Mockito.mock(classOf[ByteBuffer]))

          when(messageSet.sizeInBytes).thenReturn(0)
          when(messageSet.size).thenReturn(0)
          when(messageSet.iterator).thenReturn(messages.iterator)

          messageSet
        }
        when(fetchResponse.messageSet(any(classOf[String]), any(classOf[Int]))).thenReturn(messageSet)

        fetchResponse
      }

      when(sc.fetch(any(classOf[FetchRequest]))).thenReturn(fetchResponse)

      override def fetch(request: FetchRequest): FetchResponse = {
        sc.fetch(request)
      }
    }
  }
}