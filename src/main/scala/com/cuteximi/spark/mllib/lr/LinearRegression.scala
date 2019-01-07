package com.cuteximi.spark.mllib.lr

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel,LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LinearRegression {

  def main(args: Array[String]): Unit = {
    // 构建 Spark 对象

    val conf = new SparkConf().setAppName("linear").setMaster("local[1]")

    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    // 读取样本数据

    val data_path = "src/main/resources/lpsa.data"

    val data: RDD[String] = sc.textFile(data_path)

    val examples = data.map(line=>{
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }).cache()

    // 1 随机种子

    val train2TestData = examples.randomSplit(Array(0.8,0.2),1)

    // 迭代次数

    val numIterations = 100

    // 每次迭代的过程中，梯度算法的步长

    val stepSize = 0.1

    val minBatchFraction = 1.0

    val lrs = new LinearRegressionWithSGD()

    // 设置截距

    lrs.setIntercept(true)

    // 设置其他的参数
    lrs.optimizer.setStepSize(stepSize)
    lrs.optimizer.setMiniBatchFraction(minBatchFraction)
    lrs.optimizer.setNumIterations(numIterations)

    val model = lrs.run(train2TestData(0))

    println(model.weights)
    println(model.intercept)

    // 拿样本的数据来测试模型

    val prediction = model.predict(train2TestData(1).map(_.features))



    val predictionAndLabel = prediction.zip(train2TestData(1).map(_.label))

    val print_predict = predictionAndLabel.take(100)

    println("predict"+"\t"+"label")

    for (i <- 0 to print_predict.length-1){
      println(print_predict(i)._1+"\t"+print_predict(i)._2)
    }

    // 计算误差

    val loss = predictionAndLabel.map{
      case (p,v) =>
        val err = p -v
        Math.abs(err)
    }.reduce(_+_)

    val error = loss / train2TestData(1).count()

    println("Result: ",error)


    // 模型保存

//    val ModelPath = "src/main/resources/model01"
//
//    model.save(sc,ModelPath)
//
//    val sameModel = LinearRegressionModel.load(sc,ModelPath)

    sc.stop()


  }

}
