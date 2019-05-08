import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector, Vectors}

case class Location(lat: Double, lon: Double)

trait DistanceCalculator {
  def calculateDistanceInKilometer(srcLocation: Location, dstLocation: Location): Double
}

class DistanceCalculatorImpl extends DistanceCalculator {

  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371

  override def calculateDistanceInKilometer(srcLocation: Location, dstLocation: Location): Double = {
    val latDistance = Math.toRadians(srcLocation.lat - dstLocation.lat)
    val lngDistance = Math.toRadians(srcLocation.lon - dstLocation.lon)
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(srcLocation.lat))
        * Math.cos(Math.toRadians(dstLocation.lat))
        * sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c).toDouble
  }
}

val airbnbDataOfInterest = sc.textFile("project/cached/airbnbDataOfInterest").map(_.split("\t"))
val yelpDataOfInterest = sc.textFile("project/cached/yelpDataOfInterest").map(_.split("\t"))
val crimeDataOfInterest = sc.textFile("project/cached/crimeDataOfInterest").map(_.split("\t"))

val model = LinearRegressionModel.load(sc, "project/AirbnbLRModel")

val latitude = sys.env("LAT").toDouble
val longitude = sys.env("LONG").toDouble
val bedrooms = sys.env("BEDROOMS").toDouble
val bathrooms = sys.env("BATH").toDouble
val beds = sys.env("BEDS").toDouble

// val inputRDD = sc.parallelize([(latitude, longitude, bedrooms, bathrooms, beds)]).cache()
val homeLatitude = latitude
val homeLongitude = longitude
val homeLocation = Location(homeLatitude, homeLongitude)

// Columns of data                                                                  -> Column indices
// id, listing_url, picture_url, street, city, state, zipcode, country, latitude    -> 0-8
// longitude, property_type, room_type, accommodates, bathrooms, bedrooms, beds     -> 9-15
// bed_type, amenities, price, weekly_price, monthly_price, security_deposit        -> 16-21
// cleaning_fee, number_of_reviews, review_scores_rating, review_scores_accuracy    -> 22-25
// review_scores_cleanliness, review_scores_checkin, review_scores_communication    -> 26-28
// review_scores_location, review_scores_value, cancellation_policy                 -> 29-31
// reviews_per_month                                                                -> 32
val airbnb_in_neighborhood = airbnbDataOfInterest.
                                filter(_.length >= 33).
                                filter(row => new DistanceCalculatorImpl().calculateDistanceInKilometer(homeLocation, Location(row(8).toDouble, row(9).toDouble)) <= 2)
// Columns of data      -> Data Type
// name                 -> String
// city                 -> String
// state                -> String
// categories           -> Set[String]
// latitude             -> Double
// longitude            -> Double
// review_count         -> Double
// stars                -> Double
// is_open              -> Long
val business_in_neighborhood = yelpDataOfInterest.filter(row => new DistanceCalculatorImpl().calculateDistanceInKilometer(homeLocation, Location(row(4).toDouble, row(5).toDouble)) <= 2)
val crime_in_neighborhood = crimeDataOfInterest.filter(row => new DistanceCalculatorImpl().calculateDistanceInKilometer(homeLocation, Location(row(6).toDouble, row(7).toDouble)) <= 2)

val airbnb_enhanced_data = airbnb_in_neighborhood.
                            map(row => (row(24), row(29), 1)).
                            filter(row => !row._1.isEmpty() && !row._2.isEmpty()). //remove rows with empty cols
                            map(row => (row._1.toDouble, row._2.toDouble, 1)).
                            reduce((accum, item) => (accum._1 + item._1, accum._2 + item._2, accum._3 + item._3))

airbnb_enhanced_data.count()

val scores_rating = airbnb_enhanced_data._1/airbnb_enhanced_data._3
val scores_location = airbnb_enhanced_data._2/airbnb_enhanced_data._3

val neighborhood_business_stars = business_in_neighborhood.
                            map(row => (row(7).toDouble, 1)).
                            reduce((accum, item) => (accum._1 + item._1, accum._2 + item._2))

val avg_business_rating = neighborhood_business_stars._1/neighborhood_business_stars._2

val num_businesses_in_neighborhood = business_in_neighborhood.count()
val num_crimes_in_neighborhood = crime_in_neighborhood.count()

// price, (bathrooms,bedrooms,beds,review_scores_rating,review_scores_location,num_businesses_in_neighborhood,num_crimes_in_neighborhood,avg_business_rating)
val pointFeatures = Vectors.dense(bathrooms.toDouble, bedrooms.toDouble, beds.toDouble, scores_rating.toDouble, scores_location.toDouble, num_businesses_in_neighborhood, num_crimes_in_neighborhood, avg_business_rating)

val predictedPrice = model.predict(pointFeatures)

println("PPP" + predictedPrice.toString+ "QQQ")

exit
