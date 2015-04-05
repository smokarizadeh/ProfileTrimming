package trim
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import java.io._
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.expressions._
import akka.event.slf4j.SLF4JLogging
import com.typesafe.config._
import scala.Array.canBuildFrom


class ProfileTrimmer extends SLF4JLogging{	
	val sc = setup()
	
	
	class SimpleCSVHeader(header:Array[String]) extends Serializable {
		val index = header.zipWithIndex.toMap
		def apply(array:Array[String], key:String):String = array(index(key))
	}
	
	def setup() : SparkContext = {
	    val confProp = ConfigFactory.load()
	    val numIter = confProp.getInt("spark.sql.numIter")
	    val spark_master = confProp.getString("spark.sql.spark_master")
	    val numRows = confProp.getString("spark.sql.numRows")
	    val execMem = confProp.getString("spark.sql.execMem")
	   
	    val conf = new SparkConf().setAppName("ProifleTrimmer").setMaster(spark_master)
		.set("spark.executor.memory", execMem)
		.set("spark.deploy.defaultCores" , "4")
		.set("spark.deploy.spreadOut", "true")
		.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		.set("spark.logConf", "true") 
		
		new SparkContext(conf)
	}
	
	/**
	 * Loading profiles from CSV and removing the header line
	 */
	def loadProfiles(profileCSV : String ) : org.apache.spark.rdd.RDD[Array[Int]] = {
		
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
			val arr_numeric : Array[Int] = Array.ofDim[Int](arr.length)
			for (i<-0 to arr.length-1) 
				 if (i == 0) 
					 arr_numeric(i)  = arr(i).substring(1, arr(i).length-1).toInt
				 else	 
					arr_numeric (i) = arr(i).toInt
			arr_numeric		
		}
	
	}
	
	
	/**
	 * Measuring avg.rating of each item,
	 * @return : 
	 */
	def measureAvgRating(ratings :  org.apache.spark.rdd.RDD[Array[Int]]) :  org.apache.spark.rdd.RDD[(Int, Double)] = {
		ratings.flatMap{arr => 
				for (i<-1 to arr.length-1) yield { //skip cell zero as it contains profileid
					(i, arr(i))  //item id, rating
				}
			}
		.groupBy(id_rating => id_rating._1) //group by item id
		.map {gr_ratings =>
			 val size = gr_ratings._2.size
			 var sum = 0.0
			 gr_ratings._2.foreach(sum += _._2) 
			 (gr_ratings._1, sum / size)
			} // (itemid, seq(ratings), size)
	}
	
//	def main(args: Array[String]) {
//		val strFile = "/Users/shahab/BackgroundStudies/NetflixDataset//Enriched_Netflix_Dataset//head_URM_100.csv"
//		println("Hello, world!")
//		val rddProfiles = loadProfiles(strFile)
//		println ("Count " + rddProfiles.count)
//    }
	
}