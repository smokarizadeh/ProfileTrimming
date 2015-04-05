package test_ml

import org.scalatest.matchers._
import org.scalatest._
import ml.ItemBasedRecommender
import ml.Strategy
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext._ 
import org.apache.spark.rdd._
import org.apache.spark.rdd.PairRDDFunctions

class ItemRecSpec extends FlatSpec with Matchers {
	val rec = new ItemBasedRecommender
	val strategy = new Strategy
	
	it  should " load 99 profiles" in {
		val strFile = "/Users/shahab/BackgroundStudies/NetflixDataset//Enriched_Netflix_Dataset//head_URM_100.csv"
		val all_ratings = rec.loadRatings(strFile).map(_._2)
		all_ratings.map(_.user).distinct.count should be (99)
		
	}
	
	it should " find highly rated items for each user successfully " in {
		val seqRatings = Seq(new Rating(1,1,5), new Rating(1,2, 3), new Rating(1,3, 2), new Rating(1,4,1),
							 new Rating(2,1,2), new Rating(2,2, 3), new Rating(2,3, 4), new Rating(2,4,0),
							 new Rating(3,1,1), new Rating(3,2, 1), new Rating(3,3, 2), new Rating(3,4,3))
		
		val rdd = rec.sc.parallelize(seqRatings)
		val thresh = 3
		val filt_ratings = strategy.filterNonHighRatings (rdd, thresh)
		val sorted = filt_ratings.keyBy(_.user).lookup(2)
		sorted.length should be (thresh)
		sorted(0).user should be (2)
		sorted(0).product should be (3)
		sorted(0).rating should be (4)
		
		val sorted3 = filt_ratings.keyBy(_.user).lookup(3)
		sorted3.length should be (thresh)
		sorted3(0).user should be (3)
		sorted3(0).product should be (4)
		sorted3(0).rating should be (3)

		val sorted1 = filt_ratings.keyBy(_.user).lookup(1)
		sorted1(0).user should be (1)
		sorted1(0).product should be (1)
		sorted1(0).rating should be (5)
	}
	
	it should " stop Spark sucessfully" in {
		rec.sc.stop
	}
}