package com.cuteximi.spark.mllib.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 逻辑回归
  *
  * 降维,升维有代价,计算复杂度变大了
  *
  */
object LogisticRegression03 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark").setMaster("local[3]")
    val sc = new SparkContext(conf)
    // 解决线性不可分我们来升维,升维有代价,计算复杂度变大了
    val inputData = MLUtils.loadLibSVMFile(sc, "src/main/resources/healthStatus03.txt")
      .map { labelPoint =>
        val label = labelPoint.label
        val feature = labelPoint.features
        val array = Array(feature(0), feature(1), feature(0) * feature(1))
        val convertFeature = Vectors.dense(array)
        new LabeledPoint(label, convertFeature)
      }
    val splits = inputData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))
    val lr = new LogisticRegressionWithLBFGS()
    lr.setIntercept(true)
    val model = lr.run(trainingData)
    val result = testData
      .map { point => Math.abs(point.label - model.predict(point.features)) }
    println("正确率=" + (1.0 - result.mean()))
    println(model.weights.toArray.mkString(" "))
    println(model.intercept)

  }

}
