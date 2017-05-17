package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel

/**
  * todo
  */
object TextNER extends SparkJob {
	appName = "Text NER"
	val keyspace = "wikidumps"
	val articlesTablename = "parsedwikipedia"
	val aliasesTablename = "wikipedialinks"
	var classifierModel = "wikipedia_randomforest_model_1494495223381"
	val contextSize = 20

	// $COVERAGE-OFF$
	/**
	  * Loads Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, articlesTablename)
		val aliases = sc.cassandraTable[Alias](keyspace, aliasesTablename)
		List(articles).toAnyRDD() ++ List(aliases).toAnyRDD()
	}

	/**
	  * todo
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val timestamp = System.currentTimeMillis / 1000
		output
			.fromAnyRDD[OffsetToken]()
			.head
			.saveAsTextFile(s"named_entities_$timestamp")
	}

	// $COVERAGE-ON$

	//	/**
	//	  * todo
	//	  *
	//	  * @param text
	//	  * @param tokenizer
	//	  * @return
	//	  */
	//	def generateLinkCandidates(
	//		text: String,
	//		aliasPageProbabilities: RDD[(String, (String, Double, Double))],
	//		tokenizer: IngestionTokenizer,
	//		sc: SparkContext
	//	): List[LinkCandidate] = {
	//		val tokens = sc.parallelize(tokenizer.processWithOffsets(text))
	//		tokens
	//			.keyBy(_.token)
	//			.join(aliasPageProbabilities.keyBy(_._1))
	//
	//	}

	//	/**
	//	  * todo
	//	  *
	//	  * @return
	//	  */
	//	def generateDataPoints(linkCandidates: List[LinkCandidate]): List[(LinkCandidate, LabeledPoint)] = {
	//		linkCandidates.map { linkCandidate =>
	//			(linkCandidate, LabeledPoint(0.0, new DenseVector(Array(0, 0, 0))))
	//		}
	//	}

	/**
	  * todo
	  *
	  * @param data
	  * @return
	  */
	def findNamedEntities(
		data: List[(Link, LabeledPoint)],
		classifier: RandomForestModel
	): List[Link] = {
		data.map { case (linkCandidate, LabeledPoint(label, features)) =>
			val prediction = classifier.predict(features)
			(linkCandidate, prediction)
		}.filter(_._2 == 1)
			.map(_._1)
	}

	/**
	  * todo
	  *
	  * @param text
	  * @param tokenizer
	  * @return
	  */
	def extractTokenContexts(text: String, tokenizer: IngestionTokenizer): List[Link] = {
		val offsetTokens = tokenizer.processWithOffsets(text)
		val linkCandidates = offsetTokens.map {
			offsetToken => Link(offsetToken.token, "invalid", Option(offsetToken.beginOffset))
		}
		val tokens = offsetTokens.map(_.token)

		linkCandidates.map { link =>
			val aliasTokens = tokenizer.onlyTokenize(link.alias)
			val aliasIndex = tokens.indexOfSlice(aliasTokens)
			val contextStart = Math.max(aliasIndex - contextSize, 0)
			val preAliasContext = tokens.slice(contextStart, aliasIndex)
			val contextEnd = Math.min(aliasIndex + contextSize, tokens.length)
			val postAliasContext = tokens.slice(aliasIndex + aliasTokens.length, contextEnd)
			val context = preAliasContext ++ postAliasContext
			val bag = TermFrequencyCounter.extractBagOfWords(tokenizer.process(context))
			link.copy(context = bag.getCounts())
		}
	}

	/**
	  * todo
	  *
	  * @param text                       RDD of parsed Wikipedia articles containing the links and their contexts
	  * @param documentFrequencies        RDD of document frequencies
	  * @param numDocuments               number of documents that was used to calculate the document frequencies
	  * @param documentFrequencyThreshold lower threshold for document frequencies
	  * @return RDD of tuples containing the link and the tfIdfs of ever term in its context
	  */
	def calculateTokenContextsTfidf(
		text: String,
		documentFrequencies: RDD[DocumentFrequency],
		numDocuments: Long,
		documentFrequencyThreshold: Int,
		tokenizer: IngestionTokenizer,
		sc: SparkContext
	): RDD[(Link, Map[String, Double])] = {
		val contextTermFrequencies = extractTokenContexts(text, tokenizer)
			.map(link => (link, Bag(link.context)))
			.flatMap(t => CosineContextComparator.joinableTermFrequencies(t._1, t._2))

		val contextTermFrequenciesRDD = sc.parallelize(contextTermFrequencies)

		CosineContextComparator.calculateTfidf(
			contextTermFrequenciesRDD,
			documentFrequencies,
			numDocuments,
			documentFrequencyThreshold)
			.reduceByKey(_ ++ _)
	}

	/**
	  * todo
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articlesWithTermFrequencies = sc.parallelize(input.head.asInstanceOf[RDD[ParsedWikipediaEntry]].take(100))
		val documentFrequencies = input(1).asInstanceOf[RDD[DocumentFrequency]]
		val aliases = input(2).asInstanceOf[RDD[Alias]]
		val numDocuments = articlesWithTermFrequencies.count()

		val articleTfidf = CosineContextComparator.calculateArticleTfidf(
			articlesWithTermFrequencies,
			documentFrequencies,
			numDocuments,
			DocumentFrequencyCounter.leastSignificantDocumentFrequency)

		val aliasPageProbabilities = CosineContextComparator.computeAliasProbabilities(aliases)

		val classifier = RandomForestModel.load(sc, classifierModel)
		val tokenizer = IngestionTokenizer(true, false)
		val namedEntities = articlesWithTermFrequencies.map { article =>
			val contextTfidf = calculateTokenContextsTfidf(
				article.getText(),
				documentFrequencies,
				numDocuments,
				DocumentFrequencyCounter.leastSignificantDocumentFrequency,
				tokenizer,
				sc
			)
			val linkFeatureEntries = CosineContextComparator
				.compareLinksWithArticles(contextTfidf, aliasPageProbabilities, articleTfidf)
			val data = linkFeatureEntries
				.map { case (link, featureEntry) => (link, featureEntry.labeledPoint()) }
				.collect
				.toList
			(article.title, findNamedEntities(data, classifier))
		}
		List(namedEntities).toAnyRDD()
	}
}
