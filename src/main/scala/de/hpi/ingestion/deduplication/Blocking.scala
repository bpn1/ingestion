package de.hpi.ingestion.deduplication


import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.deduplication.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._

import scala.xml.XML
import scala.collection.mutable
import scala.io.Source

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.ConfigFile
import de.hpi.ingestion.framework.SparkJob

/**
  * Created by radscheit on 28.04.17.
  */
class Blocking(
	val configFile: Option[String] = None
	val dataSources: List[String]
) extends Serializable with SparkJob {
	var config = List[ScoreConfig[String, SimilarityMeasure[String]]]()
	val settings = mutable.Map[String, String]()
	var blockingSchemes = List[BlockingScheme](new SimpleBlockingScheme)
	appName = importName


	/**
	  * Executes methods on the blocked data to gain information.
	  *
	  * @param blocks  Input data, partitioned in blocks.
	  * @param comment Note to add to the evaluated data.
	  * @return Blocks with size-value and comment.
	  */
	def evaluateBlocks(
	blocks: RDD[(List[List[String]], List[Subject])],
	comment: String
	): BlockEvaluation = {
	val dataMap = blocks
	.map(block => (block._1.map(_.mkString(" ")), block._2.size))
	.sortBy(_._2, false)
	.collect()
	.toMap
	BlockEvaluation(data = dataMap, comment = Option(comment))
	}










}

object Blocking {

}
