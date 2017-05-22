package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.CosineContextComparator.{calculateDefaultDf, calculateIdf, calculateTfidf,
transformArticleTfs}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel

/**
  * todo
  */
object TextNEL extends SparkJob {
	appName = "Text NEL"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	var modelFunction = loadModel _

	/**
	  * Loads Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val tfArticles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		val documentFrequencies = sc.cassandraTable[DocumentFrequency](settings("keyspace"), settings("dfTable"))
		val aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		List(tfArticles).toAnyRDD() ++ List(documentFrequencies).toAnyRDD() ++ List(aliases).toAnyRDD()
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
		val namedEntities = output.head.asInstanceOf[RDD[Link]]
		val articlesWithLinks = output(1).asInstanceOf[RDD[Article]]
		namedEntities.saveAsTextFile(s"named_entities_$timestamp")
		articlesWithLinks.saveToCassandra(settings("keyspace"), settings("linkedArticlesTable"))
	}

	/**
	  * todo
	  *
	  * @return
	  */
	def loadModel(sc: SparkContext): RandomForestModel = {
		RandomForestModel.load(sc, settings("classifierModel"))
	}

	// $COVERAGE-ON$

	/**
	  * todo
	  *
	  * @param data
	  * @return
	  */
	def findAndLinkNamedEntities(
		data: RDD[(Link, LabeledPoint)],
		classifier: RandomForestModel
	): RDD[Link] = {
		data.map { case (linkCandidate, LabeledPoint(label, features)) =>
			val prediction = classifier.predict(features)
			(linkCandidate, prediction)
		}.filter(_._2 == 1)
			.map(_._1)
			.map { link =>
				link.context = Map[String, Int]()
				link
			}
	}

	/**
	  * todo
	  *
	  * @param
	  * @return
	  */
	def extractTokenContexts(
		tokens: List[String],
		linkCandidates: List[Link],
		tokenizer: IngestionTokenizer
	): List[Link] = {
		linkCandidates.map { link =>
			val aliasTokens = tokenizer.onlyTokenize(link.alias)
			val aliasIndex = tokens.indexOfSlice(aliasTokens)
			val contextStart = Math.max(aliasIndex - settings("contextSize").toInt, 0)
			val preAliasContext = tokens.slice(contextStart, aliasIndex)
			val contextEnd = Math.min(aliasIndex + settings("contextSize").toInt, tokens.length)
			val postAliasContext = tokens.slice(aliasIndex + aliasTokens.length, contextEnd)
			val context = preAliasContext ++ postAliasContext
			val bag = TermFrequencyCounter.extractBagOfWords(tokenizer.process(context))
			link.copy(context = bag.getCounts())
		}
	}

	/**
	  * todo
	  *
	  * @param articles                   RDD of parsed Wikipedia articles containing the links and their contexts
	  * @param documentFrequencies        RDD of document frequencies
	  * @param numDocuments               number of documents that was used to calculate the document frequencies
	  * @param documentFrequencyThreshold lower threshold for document frequencies
	  * @return RDD of tuples containing the link and the tfIdfs of ever term in its context
	  */
	def calculateTokenContextsTfidf(
		articles: RDD[Article],
		documentFrequencies: RDD[DocumentFrequency],
		aliases: RDD[Alias],
		numDocuments: Long,
		documentFrequencyThreshold: Int,
		tokenizer: IngestionTokenizer,
		defaultIdf: Double,
		sc: SparkContext
	): RDD[(Link, Map[String, Double])] = {
		val aliasPages = aliases.flatMap(alias => alias.pages.map(page => (alias.alias, page._1)))
		val contextTermFrequencies = articles
			.flatMap { case Article(title, text) =>
				val offsetTokens = tokenizer.processWithOffsets(text)
				val tokens = offsetTokens.map(_.token)
				offsetTokens.map(offsetToken => (offsetToken.token, (title, tokens, offsetToken.beginOffset)))
			}
			.join(aliasPages)
			.map { case (alias, ((title, tokens, beginOffset), page)) =>
				((title, tokens), List(Link(alias, page, Option(beginOffset), article = Option(title))))
			}.reduceByKey(_ ++ _)
			.flatMap { case ((title, tokens), linkCandidates) =>
				extractTokenContexts(tokens, linkCandidates, tokenizer)
			}
			.map(link => (link, Bag(link.context)))

		CosineContextComparator.calculateTfidf(
			contextTermFrequencies,
			numDocuments,
			defaultIdf)
			.reduceByKey(_ ++ _)
	}

	/**
	  * todo
	  *
	  * @param title
	  * @param text
	  * @param links
	  * @return
	  */
	def generateArticleWithLinks(title: String, text: String, links: List[Link]): Article = {
		var html = ""
		var processedCharacters = 0
		links
			.sortBy(_.offset)
			.foreach { link =>
				html += text.substring(processedCharacters, link.offset.get)
				processedCharacters = link.offset.get
				html += "<a href=\"" + link.page + "\">"
				html += text.substring(processedCharacters, processedCharacters + link.alias.length)
				processedCharacters += link.alias.length
				html += "</a>"
			}
		html += text.substring(processedCharacters, text.length)
		Article(title, html)
	}

	/**
	  * todo
	  *
	  * @param links
	  * @return
	  */
	def generateArticlesWithLinks(articles: RDD[Article], links: RDD[Link]): RDD[Article] = {
		val groupedLinks = links
			.map(link => (link.article.get, List(link)))
			.reduceByKey(_ ++ _)
		articles
			.map(article => (article.title, article.text))
			.leftOuterJoin(groupedLinks)
			.map { case (title, (text, linksForArticle)) =>
				linksForArticle match {
					case Some(linksList) => (title, text, linksList)
					case None => (title, text, Nil)
				}
			}
			.map((generateArticleWithLinks _).tupled)
	}

	/**
	  * todo
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articlesWithTermFrequencies = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
		val documentFrequencies = input(1).asInstanceOf[RDD[DocumentFrequency]]
		val aliases = input(2).asInstanceOf[RDD[Alias]]
		val numDocuments = articlesWithTermFrequencies.count()

		val defaultDf = calculateDefaultDf(DocumentFrequencyCounter.leastSignificantDocumentFrequency)
		val defaultIdf = calculateIdf(defaultDf, numDocuments)
		val articleTfs = transformArticleTfs(articlesWithTermFrequencies)
		val articleTfidf = calculateTfidf(articleTfs, numDocuments, defaultIdf)

		val aliasPageProbabilities = CosineContextComparator.computeAliasProbabilities(aliases)

		val classifier = modelFunction(sc)
		val tokenizer = IngestionTokenizer(true, false)
		val articles = articlesWithTermFrequencies.map(article => Article(article.title, article.getText()))

		val contextTfidf = calculateTokenContextsTfidf(
			articles,
			documentFrequencies,
			aliases,
			numDocuments,
			DocumentFrequencyCounter.leastSignificantDocumentFrequency,
			tokenizer,
			defaultIdf,
			sc
		)

		val linkFeatureEntries = CosineContextComparator
			.compareLinksWithArticles(contextTfidf, aliasPageProbabilities, articleTfidf)
		val data = linkFeatureEntries
			.map { case (link, featureEntry) => (link, featureEntry.labeledPoint()) }
		val namedEntities = findAndLinkNamedEntities(data, classifier)
		val articlesWithLinks = generateArticlesWithLinks(articles, namedEntities)

		List(namedEntities).toAnyRDD() ++ List(articlesWithLinks).toAnyRDD()
	}
}
