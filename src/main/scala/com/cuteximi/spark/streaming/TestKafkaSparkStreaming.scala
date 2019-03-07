package com.cuteximi.spark.streaming

import com.google.gson.Gson
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestKafkaSparkStreaming {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("JSONDataHandler")
    val ssc = new StreamingContext(conf, Seconds(2))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_streaming",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Set("spark_streaming_kafka_json")

    val stream = KafkaUtils.createDirectStream(ssc,kafkaParams,[0,100],)

    (ssc, topics, kafkaParams)

    //    // 获取每一行，解析 json ,拿到 imgId 字段，统计个数
    //    val json = messages.map(record => (record.value))
    //
    //    val id

    /**
      * 方法一：处理JSON字符串为case class 生成RDD[case class] 然后直接转成DataFrame
      */
    stream.map(record => handleMessage2CaseClass(record.value())).foreachRDD(rdd => {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      val df = spark.createDataFrame(rdd)
      df.show()
    })
  }




  def handleMessage2CaseClass(jsonStr: String): KafkaMessage = {
    val gson = new Gson()
    gson.fromJson(jsonStr, classOf[KafkaMessage])
  }

  case class KafkaMessage(time: String, namespace: String, id: String, region: String, value: String, valueType: String)


}
