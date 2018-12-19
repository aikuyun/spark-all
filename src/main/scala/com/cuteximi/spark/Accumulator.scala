package com.cuteximi.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.deploy.master.Master
object Accumulator {

  def main(args: Array[String]): Unit = {
    // 配置
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[1]")

    val sc = new SparkContext(conf)

    // 数据 指定两个分区
    val rdd1 = sc.textFile("src/main/resources/tq.txt",2)

    val accumulator = sc.accumulator(0)

    rdd1.foreach(x => {
      accumulator.add(1)
      println(accumulator)
    })

    println(accumulator.value)

    // 获取sqlContext
    val sqlContext = new SQLContext(sc)

    val dataFrame = sqlContext.read.json(" ")

  }

}
