import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}


val finalData = sc.wholeTextFiles("project/relevantDataRDD1").flatMap(_._2.split("\\n")).map(_.split(",")).filter(_.length==6).filter(x=> !(x(4).contains('$') || x(5).contains('$')) )

val mllibData = finalData.map(line=> LabeledPoint(line(3).toDouble, Vectors.dense(line(4).toDouble, line(5).toDouble)))
val splits = mllibData.randomSplit(Array(0.7, 0.3), seed = 11L)
val training = splits(0).cache()
val test = splits(1)
val numIterations = 3000
val stepSize = 0.000006
val model = LinearRegressionWithSGD.train(training, numIterations, stepSize)
	

// mean squared training error
val valuesAndPreds = training.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()


val valuesAndPredsTest = test.map { point =>
  val prediction = model.predict(point.features)
  (point.label, prediction)
}
val MSE_test = valuesAndPredsTest.map{ case(v, p) => math.pow((v - p), 2) }.mean()

// Creating funcion that the UI will use
def predictPrice(inp: LabeledPoint) {
	return model.predict(inp.features)
}

