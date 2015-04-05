package ml

import org.apache.spark.SparkContext._ 
import org.apache.spark.rdd._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.mllib.recommendation.Rating

class Strategy extends Serializable {
	
	/**
	 * Choosing only highly rated items
	 */
	def filterNonHighRatings(ratings : RDD[Rating], thresh : Int ) : RDD[Rating] = {
		ratings.keyBy(_.user)
		.map{
			case (userid, rate) => (userid, Seq(rate))
		}
		.reduceByKey(_++_)
		.map {
			case (userid, rates) =>
				rates.sortWith((r1,r2) => r1.rating > r2.rating).take(thresh)
		}
		.flatMap( rate => rate)
	}
}