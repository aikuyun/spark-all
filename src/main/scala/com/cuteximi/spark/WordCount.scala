package com.cuteximi.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    // 配置本地模式
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[1]")
    val sc = new SparkContext(conf)

    // 获取
    val lines = sc.textFile("src/main/resources/words.txt")

    // map - 分组 - 输出
    println(lines.flatMap(_.split(" ").map((_,1)).groupBy(_._1).map(x => (x._1,x._2.length))))

    // 关闭资源
    sc.stop()

  }

}
