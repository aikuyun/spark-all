package com.cuteximi.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag
import scala.util.Random

object Skeiw {

  /**
    *
  方案使用场景：
    对RDD执行reduceByKey等聚合类shuffle算子或者在Spark SQL中使用group by语句进行分组聚合时，比较适用这种方案。
方案实现思路：
   1、这个方案的核心实现思路就是分别进行两次聚合。第一次是局部聚合，先给每个key都打上一个随机数，比如2以内的随机数，此时原先一样的key就变成不一样的了，比如(hello, 1) (hello, 1) (hello, 1) (hello, 1)，就会变成(1_hello, 1) (1_hello, 1) (2_hello, 1) (2_hello, 1)。
   2、接着对打上随机数后的数据，执行reduceByKey等聚合操作，进行局部聚合，那么局部聚合结果，就会变成了(1_hello, 2) (2_hello, 2)。然后将各个key的前缀给去掉，就会变成(hello,2)(hello,2)，再次进行全局聚合操作，就可以得到最终结果了，比如(hello, 4)。
实现原理：
    1、将原本相同的key通过附加随机前缀的方式，变成多个不同的key，就可以让原本被一个task处理的数据分散到多个task上去做局部聚合，进而解决单个task处理数据量过多的问题。
   2、接着去除掉随机前缀，再次进行全局聚合，就可以得到最终的结果。
    * @param args
    */

  def main(args: Array[String]): Unit = {

    // 配置 spark

    val conf = new SparkConf().setAppName("Top").setMaster("local[1]")

    // 获取上下文

    val sc = new SparkContext(conf)

    val list = List(
      "xiao xin xin shan xiao dan",
      "yan xiao xiao zeng xiao wei",
      "xiao hui tian tian xiao shan",
      "xiao li shan xiao"
    )

    val listRDD = sc.parallelize(list)
    val wordsRDD = listRDD.flatMap(line => line.split(" "))
    val pairsRDD = wordsRDD.map(word => (word, 1))
    /**
      * 因为发生数据倾斜之后，我们需要对发生的key进行判定，使用sample算子来进行抽样判定
      */
    val sampleRDD = pairsRDD.sample(true, 0.6)
    val sortedRDD = sampleRDD.reduceByKey(_+_).sortBy(t => t)(new Ordering[(String, Int)]{
      override def compare(x: (String, Int), y: (String, Int)) = {
        y._2 - x._2
      }
    }, ClassTag.Object.asInstanceOf[ClassTag[(String, Int)]])

    //提取排名前2的数据
    val dataskewKeys = sortedRDD.take(2).map(t => t._1)

    val dsBC = sc.broadcast(dataskewKeys)

    println("发生数据倾斜的key：" + dataskewKeys.mkString("[", ",", "]"))
    //将发生数据倾斜的key进行打散 加随机前缀
    val prefixPairRDD = pairsRDD.map(t => {
      if(dsBC.value.contains(t._1)) {//该数据发生数据倾斜的key
      val random = new Random()
        val randomIndex = random.nextInt(2)
        val randomKey = randomIndex + "_" + t._1
        (randomKey, t._2)
      } else {
        t
      }
    })
    println("添加随机前缀之后的数据情况：")
    prefixPairRDD.foreach(println)
    //局部聚合
    val partRDD = prefixPairRDD.reduceByKey(_+_)
    println("局部聚合之后的数据情况：")
    partRDD.foreach(println)
    println("-------------全局聚合-----------------")
    //去掉前缀
    val unPrifixRDD = partRDD.map(t => {
      if(t._1.contains("_")) {
        //0_xiao
        (t._1.split("_")(1), t._2)
      } else {
        t
      }
    })
    println("去掉随机前缀之后的rdd：")
    unPrifixRDD.foreach(println)
    //全局聚合
    println("全局聚合之后的数据情况")
    unPrifixRDD.reduceByKey(_+_).foreach(println)
    sc.stop
  }

}
