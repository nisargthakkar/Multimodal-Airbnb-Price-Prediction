import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}


// val finalData = sc.wholeTextFiles("project/airbnbPredictionData").flatMap(_._2.split("\\n")).map(_.split(",")).filter(_.length==6).filter(x=> !(x(4).contains('$') || x(5).contains('$')) )
// airbnbid,latitude,longitude,bathrooms,bedrooms,beds,review_scores_rating,review_scores_location,num_businesses_in_neighborhood,num_crimes_in_neighborhood,price,avg_business_rating
val finalData = sc.textFile("project/airbnbPredictionData").
                  flatMap(_.split("\\n")).
                  map(_.split(",")).
                  filter(_.length == 12).
                  filter(_.forall(!_.isEmpty())).
                  filter(row => !row(10).contains('$'))

val mllibData = finalData.map(row => LabeledPoint(row(10).toDouble, Vectors.dense(row(3).toDouble, row(4).toDouble, row(5).toDouble, row(6).toDouble, row(7).toDouble, row(8).toDouble, row(9).toDouble, row(11).toDouble)))
val splits = mllibData.randomSplit(Array(0.7, 0.3), seed = 11L)
val training = splits(0).cache()
val test = splits(1)
val numIterations = 100000
val stepSize = 0.000008
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