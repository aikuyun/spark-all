package com.cuteximi.spark

import org.apache.spark.{SparkConf, SparkContext}

object PipeLineTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.setMaster("local").setAppName("pipeline")

    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(1,2,3,4))

    val rdd1 = rdd.map { x => {
      println("map--------"+x)
      x
    }}
    val rdd2 = rdd1.filter { x => {
      println("filter********"+x)
      true
    }}
    rdd2.collect()
    sc.stop()
  }

}
