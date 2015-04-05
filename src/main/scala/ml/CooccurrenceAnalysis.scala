package ml

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.util.Random
import scala.collection.mutable
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.mllib.recommendation.Rating


/**
 * The code is originally developed by Sebastian Schelter: https://gist.github.com/sscdotopen/8314254 
 */
 //case class Interaction(val user: Int, val item: Int, val rating : Int)
 
class CooccurrenceAnalysis extends Serializable {
	 class CoocRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
      kryo.register(classOf[Rating])
    }
  }
  
  
  def computeCooccurrenceMatrix( maxSimilarItemsPerItem : Int , rawInteractions :  org.apache.spark.rdd.RDD[Rating], sc : SparkContext) : 
	  org.apache.spark.rdd.RDD[(Int, Int, Double)] = {
	  
	    val numSlices = 2
	    
	    val seed = 12345
	 
	    val interactions = rawInteractions
	    //val interactions = downSample(sc, rawInteractions, maxSimilarItemsPerItem, seed)
	    interactions.cache()
	 
	    println ("RawInteravtionSize = " + rawInteractions.count + ", down sampling = "  + interactions.count)
	    
	    val numInteractions = interactions.count()
	 
	    val numInteractionsPerItem =
	      countsToDict(interactions.map(interaction => (interaction.product, 1)).reduceByKey(_ + _))
	      
	    sc.broadcast(numInteractionsPerItem)
	 
	    /* create the upper diagonal half of the cooccurrence matrix:
	       emit and count all pairs of items that cooccur in the interaction history of a user */
	    val cooccurrences = interactions.groupBy(_.user).flatMap({ case (user, history) => {
	      for (interactionA <- history; interactionB <- history; if interactionA.product > interactionB.product)
	        yield { ((interactionA.product, interactionB.product), 1l) }
	    }}).reduceByKey(_ + _)
	 
	    /* compute the pairwise loglikelihood similarities for the upper half of the cooccurrence matrix */
	    val similarities = cooccurrences.map({ case ((itemA, itemB), count) => {
	 
	      val interactionsWithAandB = count
	      val interactionsWithAnotB = numInteractionsPerItem(itemA) - interactionsWithAandB
	      val interactionsWithBnotA = numInteractionsPerItem(itemB) - interactionsWithAandB
	      val interactionsWithNeitherAnorB = numInteractions - numInteractionsPerItem(itemA) - 
	                                           numInteractionsPerItem(itemB) + interactionsWithAandB
	 
	      val logLikelihood = LogLikelihood.logLikelihoodRatio(interactionsWithAandB, interactionsWithAnotB,
	                                                           interactionsWithBnotA, interactionsWithNeitherAnorB)
	      val logLikelihoodSimilarity = 1.0 - 1.0 / (1.0 + logLikelihood)
	 
	      ((itemA, itemB), logLikelihoodSimilarity)
	    }})
	 
	    val bidirectionalSimilarities = similarities.flatMap { case ((itemA, itemB), similarity) =>
	      Seq((itemA, (itemB, similarity)), (itemB, (itemA, similarity))) }
	 
	    val order = Ordering.fromLessThan[(Int, Double)]({ case ((itemA, similarityA), (itemB, similarityB)) => {
	      similarityA > similarityB
	    }})
	 
	    /* use a fixed-size priority queue to only retain the top similar items per item */
	    val topKSimilarities = bidirectionalSimilarities.groupByKey().flatMap({ case (item, candidates) => {
	 
	      for ((similarItem, similarity) <- topK(candidates.toSeq, order, maxSimilarItemsPerItem))
	      	yield { (item , similarItem  , similarity)}
	        //yield { item + "\t" + similarItem  + "\t" + similarity}
	    }})
	 
	    topKSimilarities.setName("CooccurrenceMatrix")
	    
	    topKSimilarities
  }
 
  def topK(candidates: Seq[(Int, Double)], order: Ordering[(Int, Double)], k: Int) = {
    val queue = new mutable.PriorityQueue[(Int,Double)]()(order)
 
    candidates.foreach({ candidate => {
      if (queue.size < k) {
        queue.enqueue(candidate)
      } else {
        if (order.lt(candidate, queue.head)) {
          queue.dequeue()
          queue.enqueue(candidate)
        }
      }
    }})
    queue.dequeueAll
  }
 
 
  def downSample(sc:SparkContext, interactions: RDD[Rating], maxInteractionsPerUserOrItem: Int, seed: Int) = {
 
    val numInteractionsPerUser = 
      countsToDict(interactions.map(interaction => (interaction.user, 1)).reduceByKey(_ + _))
    sc.broadcast(numInteractionsPerUser)
 
    val numInteractionsPerItem =
      countsToDict(interactions.map(interaction => (interaction.product, 1)).reduceByKey(_ + _))
    sc.broadcast(numInteractionsPerItem)
 
    println ("numInteractionsPerUser: " + numInteractionsPerUser + " , numInteractionsPerItem= " + numInteractionsPerItem)
    
    
    
    def hash(x: Int): Int = {
      val r = x ^ (x >>> 20) ^ (x >>> 12)
      r ^ (r >>> 7) ^ (r >>> 4)
    }
 
    /* apply the filtering on a per-partition basis to ensure repeatability in case of failures by
       incorporating the partition index into the random seed */
    interactions.mapPartitionsWithIndex({ case (index, interactions) => {
 
      val random = new Random(hash(seed ^ index))
 
      interactions.filter({ interaction => {
        val perUserSampleRate = math.min(maxInteractionsPerUserOrItem, numInteractionsPerUser(interaction.user)) / 
                                  numInteractionsPerUser(interaction.user)
        val perItemSampleRate = math.min(maxInteractionsPerUserOrItem, numInteractionsPerItem(interaction.product)) / 
                                  numInteractionsPerItem(interaction.product)
 
        random.nextDouble() <= math.min(perUserSampleRate, perItemSampleRate)
      }})
    }})
 
  }
 
  def countsToDict(tuples: RDD[(Int, Int)]) = {
    tuples.toArray().foldLeft(Map[Int, Int]()) { case (table, (item, count)) => table + (item -> count) }
  }
 
}
 
object LogLikelihood {
 
  def logLikelihoodRatio(k11: Long, k12: Long, k21: Long, k22: Long) = {
    val rowEntropy: Double = entropy(k11 + k12, k21 + k22)
    val columnEntropy: Double = entropy(k11 + k21, k12 + k22)
    val matrixEntropy: Double = entropy(k11, k12, k21, k22)
    if (rowEntropy + columnEntropy < matrixEntropy) {
      0.0
    } else {
      2.0 * (rowEntropy + columnEntropy - matrixEntropy)
    }
  }
 
  private def xLogX(x: Long): Double = {
    if (x == 0) {
      0.0
    } else {
      x * math.log(x)
    }
  }
 
  private def entropy(a: Long, b: Long): Double = { xLogX(a + b) - xLogX(a) - xLogX(b) }
 
  private def entropy(elements: Long*): Double = {
    var sum: Long = 0
    var result: Double = 0.0
    for (element <- elements) {
      result += xLogX(element)
      sum += element
    }
    xLogX(sum) - result
  }
}