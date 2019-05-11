/* Code to train machine learning models on combined data of three datasets - airbnb, yelp and Toronto crime*/

import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils

// val finalData = sc.wholeTextFiles("project/airbnbPredictionData").flatMap(_._2.split("\\n")).map(_.split(",")).filter(_.length==6).filter(x=> !(x(4).contains('$') || x(5).contains('$')) )
// airbnbid,latitude,longitude,bathrooms,bedrooms,beds,review_scores_rating,review_scores_location,num_businesses_in_neighborhood,num_crimes_in_neighborhood,price,avg_business_rating
val finalData = sc.textFile("project_data/airbnbPredictionData").
                  flatMap(_.split("\\n")).
                  map(_.split(",")).
                  filter(_.length == 12).
                  filter(_.forall(!_.isEmpty())).
                  filter(row => !row(10).contains('$'))

val mllibData = finalData.map(row => LabeledPoint(row(10).toDouble, Vectors.dense(row(3).toDouble/12.5, row(4).toDouble/13, row(5).toDouble/16, row(6).toDouble/100, row(7).toDouble/10, row(8).toDouble/3045, row(9).toDouble/28133, row(11).toDouble/4.5)))
val splits = mllibData.randomSplit(Array(0.8, 0.2), seed = 11L)
val training = splits(0).cache()
val test = splits(1)

/*
val numIterations = 100000
val stepSize = 0.00000006
val model = LinearRegressionWithSGD.train(training, numIterations, stepSize)
*/

//bathroom categories: 18 max val 12.5
//bedroom categories: 14 max val 13
//beds categories: 23, max_val - 16 
//review_scores_rating: max val 100
//review_scores_location: max val 10
//num_businesses_in_neighborhood max val - 3045
//num_crimes_in_nneighborhood 28133
//average_business_rating 4.5
//price 998

val categoricalFeaturesInfo = Map[Int, Int]()
val impurity = "variance"
val maxDepth = 5
val maxBins = 32

val model = DecisionTree.trainRegressor(training, categoricalFeaturesInfo, impurity, maxDepth, maxBins)


/*
// Train a RandomForest model.
// Empty categoricalFeaturesInfo indicates all features are continuous.
val numClasses = 2
val categoricalFeaturesInfo = Map[Int, Int]()
val numTrees = 30 // Use more in practice.
val featureSubsetStrategy = "auto" // Let the algorithm choose.
val impurity = "variance"
val maxDepth = 4
val maxBins = 32

val model = RandomForest.trainRegressor(training, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
*/


/*
// Train a GradientBoostedTrees model.
// The defaultParams for Regression use SquaredError by default.
val boostingStrategy = BoostingStrategy.defaultParams("Regression")
boostingStrategy.numIterations = 10 // Note: Use more iterations in practice.
boostingStrategy.treeStrategy.maxDepth = 5
// Empty categoricalFeaturesInfo indicates all features are continuous.
boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

val model = GradientBoostedTrees.train(training, boostingStrategy)
*/



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

model.save(sc, "project/AirbnbMLModel")
