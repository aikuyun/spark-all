package com.cuteximi.spark.mllib.lr

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionWithSGD}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 逻辑回归
  *
  * 二分类
  */
object LogisticRegression02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    // 加载数据
    val inputData = MLUtils.loadLibSVMFile(sc,"src/main/resources/healthStatus02.txt")

    val splits = inputData.randomSplit(Array(0.8,0.2),1) // 指定随机比例和随机种子，一旦随机种子指定，结果不会变（在不改变其他参数的前提下）

    val (trainingData, testData) = (splits(0),splits(1))

    // 支持二元和多元
    val lr = new LogisticRegressionWithSGD()

    // 指定斜距, 也就是 W0
    lr.setIntercept(true)

    val model = lr.run(trainingData)

    val result= testData.map { point => Math.abs(point.label - model.predict(point.features)) }

    println("正确率=" + (1.0 - result.mean()))

    println(model.weights.toArray.mkString(" "))

    println(model.intercept)


  }

}
