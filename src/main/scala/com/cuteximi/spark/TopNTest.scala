package com.cuteximi.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object TopNTest {

  def main(args: Array[String]): Unit = {

    val num = 2

    val path = "src/main/resources/top.txt"

    // 配置 spark

    val conf = new SparkConf().setAppName("Top").setMaster("local[1]")

    // 获取上下文

    val sc = new SparkContext(conf)

    // 获取数据

    val rdd = sc.textFile(path)

    val rdd1 = rdd.filter(_.length>0).map(_.split(" ")).map(x=>(x(0).trim,x(1).trim))


    // 缓存这个rdd1
    rdd1.cache()

    //rdd1.foreach(println)

    // 按照 key 加上随机数，分区
    val rdd2 = rdd1.mapPartitions(x=>{x.map(y=>{((Random.nextInt(10),y._1),y._2)})})

    rdd2.cache()

    //rdd2.foreach(println)

    //这时，数据是这样的。
    /**
      * ((9,spark),78)
      * ((3,sql),98)
      * ((3,java),80)
      * ((6,javascript),98)
      * ((8,scala),69)
      * ((3,hadoop),87)
      * ((4,hbase),97)
      * ((6,hive),86)
      * ((7,spark),89)
      * ((6,sql),65)
      * ((1,java),45)
      * ((8,javascript),76)
      * ((4,scala),34)
      * ...
      */
    // 第一次聚合
  val rdd3 = rdd2.groupByKey().flatMap({
        case ((_,key),value) =>{
          value.toList.sorted.takeRight(2).map(value=>(key,value))
        }
      })


    rdd3.cache()

    rdd3.foreach(println)
  }

}
