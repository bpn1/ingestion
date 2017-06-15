package de.hpi.ingestion.textmining

import com.datastax.spark.connector._
import de.hpi.ingestion.dataimport.dbpedia.models.Relation
import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.Sentence
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector

/**
  * Parses all Sentences with entities from Wikipedia.
  */
object RelationClassifier extends SparkJob {
	appName = "Relation Classifier"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Parsed Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val sentences = sc.cassandraTable[Sentence](settings("keyspace"), settings("sentenceTable"))
		val relations = sc.cassandraTable[Relation](settings("keyspace"), settings("DBpediaRelationTable"))
		List(sentences).toAnyRDD() ++ List(relations).toAnyRDD()
	}

	/**
	  * Saves Sentences with entities to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		output
			.fromAnyRDD[Sentence]()
			.head
			.saveToCassandra(settings("keyspace"), settings("sentenceTable"))
	}
	// $COVERAGE-ON$

	def parseBagOfWordsForRelations(relations: RDD[Relation], sentences: RDD[Sentence]): RDD[String, List[String]] = {
		relations
			.map(rel => (List(rel.subjectentity, rel.objectentity), rel.relationtype))
			.join(
				sentences
					.map(sentence => (sentence.entities.map(_.entity), sentence.bagofwords))
					.filter(_._1.length == 2)
			).values
			.map { case (relation, bagOfWords) => (relation, List(bagOfWords)) }
			.reduceByKey(_ ++ _)
	}

	/**
	  * Parses all Sentences with at least 2 entities from every Wikipedia article and puts them into an RDD.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array()): List[RDD[Any]] = {
		val sentences = input.fromAnyRDD[Sentence]().head
		val relations = input.fromAnyRDD[Relation]().last

		val relationsWithBagOfWords = parseBagOfWordsForRelations(relations, sentences)

		relationsWithBagOfWords.map { case (relation, bagOfWords) =>
			val hashingTF = new HashingTF()
			val tf: RDD[Vector] = hashingTF.transform(bagOfWords)

			// While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
			// First to compute the IDF vector and second to scale the term frequencies by IDF.
			tf.cache()
			val idf = new IDF().fit(tf)
			val tfidf: RDD[Vector] = idf.transform(tf)

			// spark.mllib IDF implementation provides an option for ignoring terms which occur in less than
			// a minimum number of documents. In such cases, the IDF for these terms is set to 0.
			// This feature can be used by passing the minDocFreq value to the IDF constructor.
//			val idfIgnore = new IDF(minDocFreq = 2).fit(tf)
//			val tfidfIgnore: RDD[Vector] = idfIgnore.transform(tf)
			(relation, tf, idf)
		}
		List(sentences).toAnyRDD()
	}
}