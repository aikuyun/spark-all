package com.cuteximi.spark.mllib.lr

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegressionOnLine {

  def main(args: Array[String]): Unit = {

    //有两个参数，第一个参数是 libsvm 的训练样本,第二个参数是保存路径

    if(args.length!=2){
      println("请输入两个参数 第一个参数是libsvn的训练样本,第二个参数是保存路劲")
      System.exit(0)
    }
    val data_path=args(0)
    val model_path=args(1)
    val conf = new SparkConf().setAppName("BinaryClassificationMetricsExample")
    val sc = new SparkContext(conf)
    // $example on$
    // Load training data in LIBSVM format
    //加载SVM文件
    //SVM文件格式为:Label 1:value 2:value
    //Lable只有1和0，使用逻辑回归必须这样哈
    //这种格式的数据一般使用sql就可可以构建
    //RDD[LabeledPoint]
    val data = MLUtils.loadLibSVMFile(sc, data_path)

    // Split data into training (60%) and test (40%)
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()

    // Run training algorithm to build the model
    //LBFGS是一种优化算法，作用于梯度下降法类似
    //setNumClasses表示类标签有2个
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

    // Clear the prediction threshold so the model will return probabilities
    //清楚threshold，那么模型返回值为概率-没怎么看懂哈NO!
    model.clearThreshold

    // Compute raw scores on the test set
    //结果为(预测分类概率，真实分类) 一般是预测为分类为正向1的概率
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    predictionAndLabels.collect().map(x=>{
      println(x._1+"-->"+x._2)

    })
    //模型的存储和读取
    model.save(sc,model_path)
    //LogisticRegression.load("");

    // Instantiate metrics object
    //使用了一个BinaryClassificationMetrics来评估
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    // Precision by threshold
    //是什么意思呢，是逻辑回归概率的阈值，大于它为正（1），小于它为负（0）
    //这里列出了所有阈值的p,r,f值
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    // Precision-Recall Curve
    val PRC = metrics.pr

    // F-measure

    //the beta factor in F-Measure computation.
    //beta 表示概率的阈值哈
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    // AUPRC，精度，召回曲线下的面积
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)

    // ROC Curve
    val roc = metrics.roc

    // AUROC，ROC曲线下面的面积，人称AUC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
    // $example off$
  }
}