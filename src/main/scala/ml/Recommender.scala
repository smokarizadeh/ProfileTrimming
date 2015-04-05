package ml

import org.apache.spark.SparkContext
import akka.event.slf4j.SLF4JLogging
import org.apache.spark.rdd._
import com.datastax.spark.connector.rdd.CassandraRDD
import com.typesafe.config._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkContext

/**
 * Interface for recommender systems (User based (ALS) CF, or ItemBased CF)
 */

trait Recommender  {
	//spark context
	def  sc: SparkContext
	
	//recommendation parameters
	def params : scala.collection.mutable.Map[String, String]
	
	//data repository place holder
	//def datarepo : DataRepo
	
	//setup recommender
	def setup() : SparkContext
	
	//load ratings
	def loadRatings (ratingsCSV : String) : RDD[(Long, org.apache.spark.mllib.recommendation.Rating)] 
	
	
	//training the model (with 60%-80% of data), to find the best training params, and also get measure of MAE, RMSE,...
	def findTrainPrams(ratings :  RDD[(Long, org.apache.spark.mllib.recommendation.Rating)])  

	def measurePerformance( ratings :  RDD[(Long, org.apache.spark.mllib.recommendation.Rating)]) : String 
	//override training parameters
}