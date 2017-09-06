package org.apache.samza.system.kafka

import java.util.Collections

import kafka.utils.ZkUtils
import org.apache.samza.system.StreamSpec
import org.junit.Test

class TestDeleteTopic {

  @Test
  def testDelete {
    //val bootstrapServers = "ltx1-kafka-kafka-samza-test-vip.stg.linkedin.com:10251"
    //val zkConnect = "zk-ltx1-kafkatest.stg.linkedin.com:12913/kafka-samza-test"

    val bootstrapServers = "xiliu-ld.linkedin.biz:9092"
    val zkConnect = "xiliu-ld.linkedin.biz:2181/"
    val connectZk = () => {
      ZkUtils(zkConnect, 6000, 6000, false)
    }


    val admin = new KafkaSystemAdmin("test", bootstrapServers, connectZk)
    val spec = new StreamSpec("XinyuTestDelete", "XinyuTestDelete", "test", 10, Collections.singletonMap("replication.factor", "1"))

    val created = admin.clearStream(spec)
    //val created = admin.getTopicMetadata(Set(spec.getPhysicalName))
    System.out.println(created)
  }

}
