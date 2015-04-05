package model

import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}
import java.util.Properties
import java.io._
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.log4j.PropertyConfigurator
import com.datastax.spark.connector.rdd.CassandraRDD
import akka.event.slf4j.SLF4JLogging
import com.typesafe.config._

/**
 * This class holds data used by Recommenders (both ALS and  ItemBased)
 */

@SerialVersionUID(114L)
object DataRepo extends Serializable with  SLF4JLogging{
	
	//keeping all ratings read from Cassandra
	var ratings: scala.collection.Map[Int, Seq[org.apache.spark.mllib.recommendation.Rating]] = _
    
	var rdd_ratings :  RDD[(Long, org.apache.spark.mllib.recommendation.Rating)] = _

   /**
	 * Loading profiles from CSV and removing the header line
	 */
	def loadProfiles(profileCSV : String, sc : SparkContext ) : RDD[(Long, Rating)] = {
		
		val csv = sc.textFile(profileCSV)  // original file
		val data = csv.map(line => line.split(";"). map(elem => elem.trim)) //lines in rows
		
		//filter header
		data.mapPartitionsWithIndex{ 
			(i, iterator) => 
				if (i == 0 && iterator.hasNext) { 
						iterator.next 
						iterator 
				} else iterator
	    }
		.map {
			arr =>
			val user = arr(0).substring(1, arr(0).length-1).toInt
			for (i<-1 to arr.length-1) yield {( 
					(i%10L, new Rating (user, i, arr(i).toInt))
			)}
		}
		.flatMap(tuple => tuple)
	
	}
  
}