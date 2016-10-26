package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.{ KafkaUtils, OffsetRange, HasOffsetRanges }
import org.apache.spark.storage.StorageLevel
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD

object KafkaReceiver {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().
      setAppName("DirectKafkaWordCount").
      setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val kafkaParams = PropertiesLoader.loadAsMap("kafka.properties")
    val appParams = PropertiesLoader.loadAsMap("app.properties")
    val topic = appParams.get("topic").get
    val topics = Array(topic).toSet

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    messages.map(_._2).saveAsTextFiles(topic, "txt")

    var offsetRanges = Array[OffsetRange]()
    messages.transform{ rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
      rdd
    }.map(_._2).print()

    ssc.start()
    ssc.awaitTermination()
  }
}