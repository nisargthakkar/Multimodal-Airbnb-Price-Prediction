//input is ranges forr #crimes, review_score_rating and avg_business rating
//output is a list of locations which match the filters

val finalData = sc.textFile("project_data/airbnbPredictionData")

val fd1 = finalData.map(x => x.split(","))

val max_crime = sys.env("MAX_CRIME").toDouble //2600
val min_crime = sys.env("MIN_CRIME").toDouble //2000

val min_avg_business_rating = sys.env("MIN_BR").toDouble //4
val max_avg_business_rating = sys.env("MAX_BR").toDouble //5

val min_review_rating = sys.env("MIN_REV").toDouble //90
val max_review_rating = sys.env("MAX_REV").toDouble //100

val fd1 = finalData.map(x => x.split(",")).filter(_.length == 12).filter(_.forall(!_.isEmpty())).filter(row => !row(10).contains('$'))

val fd2 = fd1.filter(x => x(9).toDouble > min_crime && x(9).toDouble < max_crime)
val fd3 = fd2.filter(x => x(11).toDouble > min_avg_business_rating && x(11).toDouble < max_avg_business_rating)
val fd4 = fd3.filter(x => x(6).toDouble > min_review_rating && x(6).toDouble < max_review_rating)

//print all the locations which pass the filters - these will be shown on the map
fd4.collect().foreach(a => println(a.mkString(",")))

exit
