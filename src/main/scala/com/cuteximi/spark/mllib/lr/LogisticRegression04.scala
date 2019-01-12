package com.cuteximi.spark.mllib.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegression04 {

  // com.cuteximi.spark.mllib.lr.LogisticRegression04
  def main(args: Array[String]) {

    if(args.length!=2){
      println("请输入两个参数 第一个参数是libsvn的训练样本,第二个参数是保存路劲")
      System.exit(0)
    }
    val data_path=args(0)
    val model_path=args(1)

      // 拿到集群上跑的
    val conf = new SparkConf().setAppName("spark-ml")
    //.setMaster("local[3]")
    val sc = new SparkContext(conf)

    val inputData: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, data_path)

    val splits = inputData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val lr = new LogisticRegressionWithLBFGS()

    lr.setIntercept(true)

    //    val model = lr.run(trainingData)
    //    val result = testData
    //      .map{point=>Math.abs(point.label-model.predict(point.features)) }
    //    println("正确率="+(1.0-result.mean()))
    //    println(model.weights.toArray.mkString(" "))
    //    println(model.intercept)

    val model = lr.run(trainingData).clearThreshold()

    val errorRate = testData.map { p =>
      val score = model.predict(p.features)
      // 癌症病人宁愿错判断出得癌症也别错过一个得癌症的病人
      val result = score > 0.3 match {
        case true => 1;
        case false => 0
      }
      Math.abs(result - p.label)
    }.mean()

    println(1 - errorRate)

    model.save(sc,model_path)

    // 0.7530925284512618 score > 0.5
    // 0.7213251206925253 score > 0.4
    // 0.6706094956464594 score > 0.3
    // 可以看出，为了
  }

}
