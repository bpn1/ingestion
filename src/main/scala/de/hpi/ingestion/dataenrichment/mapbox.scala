package de.hpi.ingestion.deduplication

import de.hpi.ingestion.datalake.models._
import de.hpi.ingestion.datalake.SubjectManager
import de.hpi.ingestion.deduplication.similarity._
import de.hpi.ingestion.deduplication.models._
import de.hpi.ingestion.implicits.CollectionImplicits._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.driver.core.utils.UUIDs

import scala.xml.XML
import scala.collection.mutable
import scala.io.Source

class MapboxDataEnrichment() extends Serializable {

}

object MapboxDataEnrichment {}