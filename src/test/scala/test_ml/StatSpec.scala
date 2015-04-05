package test_ml


import trim.ProfileTrimmer
import org.scalatest.matchers._
import org.scalatest._

class StatSpec extends FlatSpec with Matchers {
//	val pt = new ProfileTrimmer
//	"RDD " should " have 99 records" in { 
//		
//		val strFile = "/Users/shahab/BackgroundStudies/NetflixDataset//Enriched_Netflix_Dataset//head_URM_100.csv"
//		val rdd = pt.loadProfiles(strFile)
//		val count = rdd.count
//		count should be (99)
//	}
//	
//	"RDD " should " calculate averages correctly " in {
//		val sc = pt.sc
//		val arr =  Array.ofDim[Int] (3,4)
//		arr(0) = Array(0,1,0,1)
//		arr(1) = Array(1,1,0,0)
//		arr(2) = Array(2,1,2,0)
//		val rdd = sc.parallelize(arr)
//		val avg = pt.measureAvgRating(rdd).collect.toMap
//		avg.size should be (3)
//		avg.get(1).get should equal (1.0)
//		avg.get(2).get should be > 0.66
//		avg.get(3).get should be > 0.33
//	}
	
}