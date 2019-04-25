import org.apache.spark.rdd.RDD

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

// SAMPLE POINT TO CALCULATE DISTANCE FROM. WILL BE INPUT FROM THE USER
val homeLatitude = 43.726377
val homeLongitude = -79.380968
val homeLocation = Location(homeLatitude, homeLongitude)

// AIRBNB DATA
// get data
val airbnbDataText = sc.wholeTextFiles("project/toronto_data")

// Columns of data                                                                  -> Column indices
// id, listing_url, picture_url, street, city, state, zipcode, country, latitude    -> 0-8
// longitude, property_type, room_type, accommodates, bathrooms, bedrooms, beds     -> 9-15
// bed_type, amenities, price, weekly_price, monthly_price, security_deposit        -> 16-22
// cleaning_fee, number_of_reviews, review_scores_rating, review_scores_accuracy    -> 23-26
// review_scores_cleanliness, review_scores_checkin, review_scores_communication    -> 27-29
// review_scores_location, review_scores_value, cancellation_policy                 -> 30-32
// reviews_per_month                                                                -> 33
val airbnbDataOfInterestWithHeaders = airbnbDataText.
                                        map(_._2).
                                        flatMap(_.split("\\n")).
                                        filter(row => row != null && row.length >= 61).
                                        map(row => {
                                            if (row.charAt(row.length() - 1) == ',')
                                                row.concat("0")
                                            else
                                                row
                                        }).
                                        map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).
                                        filter(row => row != null && row.length == 62). // remove in case split not done correctly
                                        map(row => Array(row(0), row(1), row(2), row(4),
                                                        row(8), row(9), row(10), row(14),
                                                        row(15), row(16), row(18), row(19),
                                                        row(20), row(21), row(22), row(23),
                                                        row(24), row(25), row(27), row(28),
                                                        row(29), row(30), row(31), row(43),
                                                        row(46), row(47), row(48), row(49),
                                                        row(50), row(51), row(52), row(57),
                                                        row(61))) // get relevant columns

// remove the title row
val airbnbHeader = airbnbDataOfInterestWithHeaders.first

val airbnbDataOfInterest = airbnbDataOfInterestWithHeaders.
                            filter(row => row(0) != "id"). // remove header row
                            filter(row => row(18) != null && row(18) != "N/A" && row(18) != "") // remove entries where price is not given


// YELP DATA
val businessDataDF = sqlContext.read.json("yelp_dataset/business.json")

// Column names in order
val yelpColumnName = Seq("name", "city", "state", "categories", "latitude", "longitude", "review_count", "stars", "is_open")
val yelpInterestedColumsParsedRDD = businessDataDF.
                                select(yelpColumnName.map(name => col(name)): _*).rdd.
                                map(row => (row.getString(0), row.getString(1), row.getString(2), row.getString(3), row.getDouble(4), row.getDouble(5), row.getLong(6), row.getDouble(7), row.getLong(8)))

val yelpInterestingCategories = Set("Restaurants", "Food", "Nightlife", "Bars", "Sandwiches", "Coffee & Tea", "Fast Food", "American (Traditional)", "Pizza", "Burgers", "Breakfast & Brunch", "Specialty Food", "American (New)", "Italian", "Mexican", "Chinese", "Bakeries", "Grocery", "Desserts", "Cafes", "Ice Cream & Frozen Yogurt")

val yelpDataOfInterest = yelpInterestedColumsParsedRDD.
                                filter(_._2 == "Toronto"). // Removing Businesses where Cities which are not Toronto
                                filter(_._3 == "ON"). // Removing Businesses where States which are not Ontario
                                filter(row => row._4 != null && yelpInterestingCategories.intersect(row._4.split(", ").toSet).size > 0). // Removing Businesses which don't have categories of or interest
                                map(row => (row._1, "TNT", row._3, row._4.split(", ").toSet, row._5, row._6, row._7, row._8, row._9)) // Transforming rows to a more convenient schema


// Crime Data
val crimeData = sc.textFile("project/MCI_2014_to_2018.csv")
val crimeHeader = crimeData.first()

// Columns of data      -> Data Type
// premisetype          -> String
// offence              -> String
// occurrenceyear       -> Int
// occurrencemonth      -> String
// MCI                  -> String
// Division             -> String
// Lat                  -> Double
// Long                 -> Double
// ObjectId             -> Long
val crimeDataOfInterest = crimeData.filter(_ != crimeHeader).
                    map(row => row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)).
                    map(x => Array(x(6), x(9), x(16), x(17), x(22), x(23), x(26), x(27), x(28))).
                    filter(_.forall(!_.isEmpty())) //remove rows with empty cols




// Filtering multimodal data

val businessesInTwoKmRadius = yelpDataOfInterest.
                                filter(row => new DistanceCalculatorImpl().calculateDistanceInKilometer(homeLocation, Location(row._5, row._6)) <= 2)

businessesInTwoKmRadius.count()

val airbnbsInTwoKmRadius = airbnbDataOfInterest.
                            filter(row => new DistanceCalculatorImpl().calculateDistanceInKilometer(homeLocation, Location(row(8).toDouble, row(9).toDouble)) <= 2)

airbnbsInTwoKmRadius.count()

val crimesInTwoKmRadius = crimeDataOfInterest.
                            filter(row => new DistanceCalculatorImpl().calculateDistanceInKilometer(homeLocation, Location(row(6).toDouble, row(7).toDouble)) <= 2)

crimesInTwoKmRadius.count()