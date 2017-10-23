/*
Copyright 2016-17, Hasso-Plattner-Institut fuer Softwaresystemtechnik GmbH

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package de.hpi.ingestion.textmining.nel

import java.io.InputStream

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.TupleImplicits._
import de.hpi.ingestion.textmining.preprocessing.{CosineContextComparator => CCC}
import de.hpi.ingestion.textmining.preprocessing._
import de.hpi.ingestion.textmining.models._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession

/**
  * Classifies the `TrieAliases` found in `TrieAliasArticles` by the `ArticleTrieSearch` and writes the positives into
  * the `foundentities` column back to the same table.
  */
class TextNEL extends SparkJob {
	import TextNEL._
	appName = "Text NEL"
	configFile = "textmining.xml"
	sparkOptions("spark.yarn.executor.memoryOverhead") = "4096"
	var aliasPageScores = Map.empty[String, List[(String, Double, Double)]]

	var trieArticles: RDD[TrieAliasArticle] = _
	var aliases: RDD[Alias] = _
	var articleTfidf: RDD[ArticleTfIdf] = _
	var numDocuments: Long = 0
	var entityLinks: List[RDD[(String, List[Link])]] = Nil

	// $COVERAGE-OFF$
	var loadModelFunction: () => PipelineModel = loadModel
	var docFreqStreamFunction: String => InputStream = AliasTrieSearch.hdfsFileStream
	/**
	  * Loads the Trie Alias Articles from the Cassandra.
	  * @param sc Spark Context used to load the RDDs
	  */
	override def load(sc: SparkContext): Unit = {
		trieArticles = sc.cassandraTable[TrieAliasArticle](settings("keyspace"), settings("NELTable"))
		numDocuments = sc
			.cassandraTable[WikipediaArticleCount](settings("keyspace"), settings("articleCountTable"))
			.first
			.count
			.toLong
		aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		articleTfidf = sc.cassandraTable[ArticleTfIdf](settings("keyspace"), settings("tfidfTable"))
	}

	/**
	  * Saves the found entities to the Cassandra.
	  * @param sc Spark Context used to connect to the Cassandra or the HDFS
	  */
	override def save(sc: SparkContext): Unit = {
		entityLinks
			.foreach(_.saveToCassandra(settings("keyspace"), settings("NELTable"), SomeColumns("id", "foundentities")))
	}

	/**
	  * Loads the Dataframe based random forest classifier model from the HDFS.
	  *
	  * @return random forest classifier model
	  */
	def loadModel(): PipelineModel = {
		PipelineModel.load(settings("classifierModel"))
	}
	// $COVERAGE-ON$

	/**
	  * Splits the Articles into n parts.
	  * @return Collection of input RDDs to be processed sequentially.
	  */
	def splitInput(): List[RDD[TrieAliasArticle]] = {
		val splitMap = Map("wikipedianel" -> 100, "spiegel" -> 20)
		val splitCount = splitMap(settings("NELTable"))
		trieArticles
			.randomSplit(Array.fill(splitCount)(1.0))
			.toList
	}

	/**
	  * Generates FeatureEntries for the aliases extracted from the TrieAliasArticles.
	  * @param sc    Spark Context used to e.g. broadcast variables
	  */
	override def run(sc: SparkContext): Unit = {
		val session = SparkSession.builder().getOrCreate()
		val articlesPartitions = splitInput()
		val articleTfidfTuples = articleTfidf.flatMap(ArticleTfIdf.unapply)

		if(aliasPageScores.isEmpty) {
			aliasPageScores = CosineContextComparator.collectAliasPageScores(aliases)
		}
		val tokenizer = IngestionTokenizer()
		entityLinks = articlesPartitions.map { partition =>
			val contextSize = settings("contextSize").toInt
			val articleAliases = partition.flatMap(extractTrieAliasContexts(_, tokenizer, contextSize))
			val aliasTfIdf = CCC.calculateTfidf(
				articleAliases,
				numDocuments,
				CCC.defaultIdf(numDocuments),
				docFreqStreamFunction,
				settings("dfFile"))
			val job = new SecondOrderFeatureGenerator
			job.featureEntries = CCC.compareLinksWithArticles(
				aliasTfIdf,
				sc.broadcast(aliasPageScores),
				articleTfidfTuples)
			job.run(sc)
			classifyFeatureEntries(job.featureEntriesWithSOF, loadModelFunction(), session)
		}
	}
}

object TextNEL {
	/**
	  * Extracts the contexts of an article's Trie Aliases.
	  *
	  * @param article   article containing the text and the Trie Aliases.
	  * @param tokenizer Tokenizer used to create the contexts
	  * @param contextSize number of tokens before and after a match considered as the context
	  * @return List of the Trie Aliases as Links and their contexts as Bags as tuples
	  */
	def extractTrieAliasContexts(
		article: TrieAliasArticle,
		tokenizer: IngestionTokenizer,
		contextSize: Int
	): List[(Link, Bag[String, Int])] = {
		val articleTokens = article.text.map(tokenizer.onlyTokenizeWithOffset).getOrElse(Nil)
		article.triealiases.map { triealias =>
			val context = TermFrequencyCounter.extractContext(articleTokens, triealias.toLink(), tokenizer, contextSize)
			(triealias.toLink().copy(article = Option(article.id)), context)
		}
	}

	/**
	  * Uses a Random Forest Model to classify the given Feature Entries and transforms them into Links.
	  *
	  * @param featureEntries RDD of Feature Entries to classify
	  * @param model          Pipeline Model of the trained Random Forest Classifier
	  * @param session        Spark Session used to access the Dataframe API
	  * @return RDD of tuples containing the article and the entities linked in the article
	  */
	def classifyFeatureEntries(
		featureEntries: RDD[FeatureEntry],
		model: PipelineModel,
		session: SparkSession
	): RDD[(String, List[Link])] = {
		import session.implicits._
		val entryDF = featureEntries.map { entry =>
			val labeledPoint = entry.labeledPointDF()
			(labeledPoint.features, labeledPoint.label, entry.article, entry.offset, entry.alias, entry.entity)
		}.toDF("features", "label", "article", "offset", "alias", "entity")

		model.transform(entryDF).rdd
			.map { row =>
				val prediction = row.getDouble(row.fieldIndex("prediction"))
				val article = row.getString(row.fieldIndex("article"))
				val offset = row.getInt(row.fieldIndex("offset"))
				val alias = row.getString(row.fieldIndex("alias"))
				val entity = row.getString(row.fieldIndex("entity"))
				val link = Link(alias, entity, Option(offset))
				(article, link, prediction)
			}.collect {
			case (article, link, prediction) if prediction == 1.0 =>
				(article, List(link))
		}.reduceByKey(_ ++ _)
			.map(_.map(identity, _.sortBy(_.offset)))
	}
}
