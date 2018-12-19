package com.cuteximi.spark

import com.cuteximi.spark.bean.Weather
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 统计天气
  *
  * 1949-10-01 14:21:02 34c
  * 1949-10-01 19:21:02 38c
  * 1949-10-02 14:01:02 36c
  * 1950-01-01 11:21:02 32c
  * 1950-10-01 12:21:02 37c
  * 1951-12-01 12:21:02 23c
  * 1950-10-02 12:21:02 41c
  * 1950-10-03 12:21:02 27c
  * 1951-07-01 12:21:02 45c
  * 1951-07-02 12:21:02 46c
  * 1951-07-03 12:21:03 47c
  *
  * 虽然可以一行代码搞定，但那样可读性太差了。
  *
  */
object Tq {


  def main(args: Array[String]): Unit = {
    // 配置
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[1]")
    val sc = new SparkContext(conf)

    RDD

    // 数据
    val rdd1 = sc.textFile("src/main/resources/tq.txt")

    val result = rdd1.map(x=>{
      val list = x.split("")
      val date = list(0)
      val time = list(1)
      val wenDu = list(2)
      val listDate = date.split("-")

      val weather = new Weather();
      weather.setYear(listDate(0))
      weather.setMonth(listDate(1))
      weather.setDay(listDate(2))
      weather.setTime(time)
      weather.setWenDu(wenDu)
    })
    

    // 按天分组，取一天的最高温度

    //val rdd2 = rdd1.map(_.split(" ")).map(x=>(x(0),x(2))).groupByKey().map(x=>{(x._1,x._2.toList.sortWith( _ > _).take(1)(0))}).cache()


    //rdd2.collect()

    // 按月分组
    // map - map - groupByKey - map - sort - print

    //val rdd3 = rdd2.map(x=>{(x._1.substring(0,7),x._2+"-"+x._1.split("-")(2))}).groupByKey().map(x=>{(x._1,x._2.toList.sortWith(_ > _).take(2))}).foreach(println)

    //rdd2.map(x=>{(x(0).substring(0,7),x(1)+"-"+x(0).split("-")(2))}).groupByKey().map(x=>{(x._1,x._2.toList.sortWith(_ > _).take(2))}).foreach(x=>println(x._1.toString+"温度最高的两天是"+x._2(0).split("-")(1)+"和"+x._2(1).split("-")(1)))


    // 获取sqlContext
    val sqlContext = new SQLContext(sc)

    val text= sqlContext.read.text("src/main/resources/tq.txt")

    //val text1= sqlContext.read.format()

    text.show()
  }

}
