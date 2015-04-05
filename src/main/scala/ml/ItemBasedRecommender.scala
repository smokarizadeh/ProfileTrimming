package ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.util.Random
import scala.collection.mutable
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import java.util.Properties
import java.io._
import com.typesafe.config._
import org.apache.log4j.PropertyConfigurator
import akka.event.slf4j.SLF4JLogging
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.storage.StorageLevel
import model.DataRepo


/**
 * This object is wrapper for item-based CF Recommender
 * The similarity between items is computed in CoocurrenceAnalysis (similarity prediction part)
 * And   recommendation part is computed here.
 */
class ItemBasedRecommender extends  SLF4JLogging  with Recommender{
	 //data repository
	 val  datarepo =  DataRepo
	 //spark context
	 val sc : SparkContext = setup()
	 //we do not use any conf
	 val params : scala.collection.mutable.Map[String, String] =  laodTrainigParams
	 // recommendation model
	 var topKSimilarities : Option[org.apache.spark.rdd.RDD[(Int, Int, Double)] ] = None  
	 
	 //correlation keyed by Item
	 var grpSimilarItem: org.apache.spark.rdd.RDD[(Int, Iterable[(Int, Int, Double)])] = _
	 
	 // The object that finds correlation between 
	 val cooccurFinder = new CooccurrenceAnalysis
	 
	// val numSlices = 2
	 val maxSimilarItemsPerItem = 20
	 val maxInteractionsPerUserOrItem = 500
	 val seed = 12345
	
	  def  setup() : SparkContext = {
		 //Loading config properties
	    val confProp = ConfigFactory.load()
	    val numIter = confProp.getInt("spark.sql.numIter")
	    val cassandra_host = confProp.getString("spark.sql.cassandra_host")
	    val spark_master = confProp.getString("spark.sql.spark_master")
	    val numRows = confProp.getString("spark.sql.numRows")
	    val execMem = confProp.getString("spark.sql.execMem")
	    val numPartitions = confProp.getString("spark.sql.numPartitions")
	    val company = confProp.getString("wrangling.rating.company")
	    val bucket = confProp.getString("wrangling.rating.bucket")
	   
	    // creating spark context
	    val conf = new SparkConf().setAppName("ItemBasedRecommender").setMaster(spark_master)
		.set("spark.cassandra.connection.host", cassandra_host)
		.set("spark.executor.memory", execMem)
		.set("spark.deploy.defaultCores" , "4")
		.set("spark.deploy.spreadOut", "true")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.cassandra.input.split.size", numPartitions)
		.set("spark.cassandra.connection.timeout_ms", "600000")
		.set("spark.logConf", "true") 
		
	     new SparkContext(conf)
	}
	 
	 def loadRatings (ratingsCSV : String) : RDD[(Long, org.apache.spark.mllib.recommendation.Rating)] = {
		 this.datarepo.loadProfiles(ratingsCSV, sc)
	 } 
	 
	 def getTrainingParams() : Map[String, String] = {
		 return this.params.toMap
	 }
	
	 //reading config params
	 def laodTrainigParams() : scala.collection.mutable.Map[String, String] ={
		val confProp = ConfigFactory.load()
		scala.collection.mutable.Map(
		    ("topK", confProp.getString("ItemBased.params.topK")),
		    ("trainingSize", confProp.getString("ItemBased.params.trainingPortion"))
	    )
	}
	 
	 def findTrainPrams(ratings :  RDD[(Long, org.apache.spark.mllib.recommendation.Rating)]) {
		 log.error("ItemBased CF does not need any tarinig config")
	 }  
	 
	  	/**
	 * This method, finds recommendations for the each user in the training dataset
	 * We use weighted sum similarities
	 * @return: (user, Seq(user, (item, recommendation score)))
	 */
 	def weightedSumRecommendations (history : org.apache.spark.rdd.RDD[Rating]) : RDD[(Int, (Int, Double))] = {
		
		//group by item
		val grpHistoryItem = history.groupBy(_.product)
		
		this.grpSimilarItem.join(grpHistoryItem).flatMap ({
			case (item_N, (similarItems, grpRatingHistory ) ) => {
				for (similarItem<-similarItems; ratingHist<-grpRatingHistory)
					//computing partial weight
					// (item_i, user, sim_N_i * rating_N ,  frequency
					yield { ( (similarItem._2, ratingHist.user),  (similarItem._3 * ratingHist.rating , 1)) }
			}
		})
 		.reduceByKey{ 
				(r1, r2) => (r1._1 + r2._1, r1._2 + r2._2)
			}
		.map ({  // divide total weights by number of weights
			case ((item_i, user_u ), (sum_weight, number_weights)) => { 
				(user_u, (item_i, sum_weight/number_weights)) 
				}
		})   
		
	}
 	
 	
 	/**
 	 * This method measure performance  (MAE) of recommendation model
 	 * It divides data into 80-20 (training-test) sets and measures MAE, RMSE on test set
 	 */
 	def measurePerformance( ratings :  RDD[(Long, org.apache.spark.mllib.recommendation.Rating)]) : String = {
 		
 		 val numRatings = ratings.count()
	     val numUsers = ratings.map(_._2.user).distinct().count() 
	     val numItem = ratings.map(_._2.product).distinct().count() 
	     val topK  = this.params.get("topK").get.toInt
	          
	     val training = ratings. filter(x => x._1 < 8).values.cache()
		 val test = ratings.filter( x => x._1 >= 8).values.cache()
			    
		 val numTest = test.count
		 val numTraining = training.count
		 
	     //training the model 
		 this.topKSimilarities = Some(cooccurFinder.computeCooccurrenceMatrix(topK, training, sc))
		 
		 this.grpSimilarItem =  this.topKSimilarities.get.groupBy(_._1).cache
		 this.grpSimilarItem.first
		
		 val recommendations = weightedSumRecommendations (test)
		.map(row => Rating(row._1, row._2._1, row._2._2))
		 
		 //measure MAE
		 val mae = measureMAE(test, recommendations, numTest)
		 val rmse = measureRMSE (test, recommendations, numTest)
		 val (tp, fp, fn) = measureAccuracy(test, recommendations)
		 
		 val precision = tp / (tp+fp)
		 val recall = tp /(tp+fn)
		 
		"Training Size : " + numTraining + ", Test Size: " + numTest + ", MAE =" +  "%1.5f".format(mae) + 
		 ", RMSE = " + "%1.5f".format(rmse) + 
		 ", precision=" + precision + " , recall = " + recall
 	}
 	
	/**
	 * Measure Accuracy, MAE
	 */
	def measureMAE( test : RDD[Rating], recommendation : RDD[Rating], numTest : Long ) :Double  = {
		
		//key (user_item)
		val rddTest = test.map(interaction => ( (interaction.user,interaction.product), interaction.rating ))
		
		//flatten recommendations
		val rddRec = recommendation.map {
			case (rec) => 
					( (rec.user,rec.product), rec.rating )
			}
		.map ({
			case (key, score) => 
					if (score.isNaN())
						(key, 0.0) //we use zero as indicator of dislike
					else
						(key, score)
		})
		
		val jointRDD =rddTest.join(rddRec)
		
		val MAE = jointRDD.values
		.map ( {
			case (x1,x2) =>  math.abs(x1- x2)
		})
		.reduce(_+_) / numTest.toDouble
		
		return MAE
	}
	
	
	/**
	 * Measure RMSE (Root Mean Squared Error)
	 */
	def measureRMSE( test : RDD[Rating], recommendation : RDD[Rating], numTest : Long ) :Double  = {
		
		//key (user_item)
		val rddTest = test.map(interaction => ( (interaction.user,interaction.product), interaction.rating ))
		
		//flatten recommendations
		val rddRec = recommendation.map  {
			case (rec) => 
				( (rec.user,rec.product), rec.rating )
		}.map ({
			case (key, score) => 
					if (score.isNaN())
						(key, 0.0)
					else
						(key,score)
		})
		
		val jointRDD =rddTest.join(rddRec)  

		val RMSE = math.sqrt(jointRDD.values
		.map ( {
			case (x1,x2) =>  (x1- x2)*(x1 - x2)
		}).reduce(_+_) / numTest)
		
		return RMSE
	}
	
	/**
	 * Measure Accuracy, MAE
	 */
	def measureAccuracy( test : RDD[Rating], recommneation : RDD[Rating] )  = {
		
		val testUI = test.map(interaction => ((interaction.user, interaction.product), 1))
		val recUI = recommneation.map{
			case (rec) =>
					((rec.user, rec.product),1)
		}
		
		//true positive list
		val tp =recUI.join(testUI).count.toDouble
		
		//false positives
		val fp = recUI.subtractByKey(testUI).count.toDouble
		
		//false negtives
		val fn = testUI.subtractByKey(recUI).count.toDouble
		
		val accuracy = tp / (tp+fp)
		val coverage = tp / (tp+fn)
		
		(tp, fp, fn)
	}
 	
	
}