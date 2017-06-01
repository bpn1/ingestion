package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.textmining.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.AliasTrieSearch.{deserializeTrie, extractAliasContext, hdfsFileStream}
import de.hpi.ingestion.textmining.CosineContextComparator.{calculateDefaultDf, calculateIdf, calculateTfidf,
transformArticleTfs, computeAliasProbabilities}
import de.hpi.ingestion.textmining.tokenizer.IngestionTokenizer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel

/**
  * Uses a trained classifier model to perform NER and NEL on Wikipedia articles.
  */
object TextNEL extends SparkJob {
	appName = "Text NEL"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	var modelFunction = loadModel _
	var trieStreamFunction = hdfsFileStream _

	/**
	  * Loads Wikipedia entries from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val tfArticles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		val aliases = sc.cassandraTable[Alias](settings("keyspace"), settings("linkTable"))
		List(tfArticles).toAnyRDD() ++ List(aliases).toAnyRDD()
	}

	/**
	  * Saves found links to named entities to the HDFS.
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
	  * Loads the random forest classifier model from the HDFS.
	  *
	  * @return random forest classifier model
	  */
	def loadModel(sc: SparkContext): RandomForestModel = {
		RandomForestModel.load(sc, settings("classifierModel"))
	}

	// $COVERAGE-ON$

	/**
	  * Uses the trained classifier to find and link named entities.
	  *
	  * @param data       link candidates with their respective labeled points
	  * @param classifier trained random forest classifier model
	  * @return identified links to named entities
	  */
	def linkNamedEntities(
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
	  * Finds all known aliases in a text.
	  *
	  * @param text             article text
	  * @param trie             trie with known aliases
	  * @param tokenizer        word tokenizer that splits text into raw words
	  * @param contextTokenizer word tokenizer for context extraction (may filter stopwords and stem)
	  * @return
	  */
	def matchTrieAliases(
		text: String,
		trie: TrieNode,
		tokenizer: IngestionTokenizer,
		contextTokenizer: IngestionTokenizer
	): List[TrieAlias] = {
		var contextAliases = List[TrieAlias]()
		val tokens = tokenizer.processWithOffsets(text)
		for(i <- tokens.indices) {
			val testTokens = tokens.slice(i, tokens.length)
			val aliasMatches = trie.matchTokens(testTokens.map(_.token))
			val offsetMatches = aliasMatches.map(textTokens => testTokens.take(textTokens.length))
			if(aliasMatches.nonEmpty) {
				val longestMatch = offsetMatches.maxBy(_.length)
				val offset = longestMatch.headOption.map(_.beginOffset)
				offset.foreach { begin =>
					val alias = text.substring(begin, longestMatch.last.endOffset)
					val context = extractAliasContext(longestMatch, tokens, i, contextTokenizer).getCounts()
					contextAliases :+= TrieAlias(alias, offset, context)
				}
			}
		}
		contextAliases
	}

	/**
	  * Finds candidates for links and extracts their term frequencies.
	  *
	  * @param articles RDD of parsed Wikipedia articles containing the links and their contexts
	  * @param aliases  RDD of all aliases
	  * @return RDD of tuples containing the link and the tfIdfs of ever term in its context
	  */
	def findLinkCandidates(
		articles: RDD[Article],
		aliases: RDD[Alias]
	): RDD[(Link, Bag[String, Int])] = {
		val tokenizer = IngestionTokenizer(false, false)
		val contextTokenizer = IngestionTokenizer(true, true)
		val trie = deserializeTrie(trieStreamFunction(settings("trieFile")))

		val aliasPages = aliases.flatMap(alias => alias.pages.map(page => (alias.alias, page._1)))
		articles
			.flatMap { article =>
				val trieAlias = matchTrieAliases(article.text, trie, tokenizer, contextTokenizer)
				trieAlias.map(trieAlias => (trieAlias.alias, (article.title, trieAlias)))
			}
			.join(aliasPages)
			.map { case (alias, ((title, trieAlias), page)) =>
				(Link(alias, page, trieAlias.offset, article = Option(title)), Bag(trieAlias.context))
			}
	}

	/**
	  * Replaces special characters with proper HTML encoding.
	  *
	  * @param text original text with special characters
	  * @return text with HTML encoded characters
	  */
	def replaceSpecialCharacters(text: String): String = {
		val htmlEncodingMap = Map("ä" -> "&auml;", "ö" -> "&ouml;", "ü" -> "&uuml;", "Ä" -> "&Auml;", "Ö" -> "&Ouml;",
			"Ü" -> "&Uuml;")
		var encodedText = text
		htmlEncodingMap.foreach { case (specialCharacter, encoding) =>
			encodedText = encodedText.replaceAll(specialCharacter, encoding)
		}
		encodedText
	}

	/**
	  * Inserts HTML links into an article text where named entities were found.
	  *
	  * @param title title of the article
	  * @param text  text of the article
	  * @param links linked named entities found in the article
	  * @return HTML containing links
	  */
	def generateArticleWithLinks(title: String, text: String, links: List[Link]): Article = {
		var html = s"<h1>$title</h1>\n"
		var processedCharacters = 0
		links
			.groupBy(_.offset)
			.toList
			.sortBy(_._1)
			.foreach { case (offset, groupedLinks) =>
				val mainLink = groupedLinks.head
				html += text.substring(processedCharacters, offset.get)
				processedCharacters = offset.get
				val pages = groupedLinks
					.map(_.page)
					.reduce(_ + "&#13;" + _)
				val wikipediaPage = "https://de.wikipedia.org/wiki/" + mainLink.page.replaceAll(" ", "_")
				html += "<a href=\"" + wikipediaPage + "\" class=\"importance " + 0 + "\" title=\"" + pages + "\">"
				html += text.substring(processedCharacters, processedCharacters + mainLink.alias.length)
				processedCharacters += mainLink.alias.length
				//html += "<ul style=\"display: none\">" + "<li>some other link</li>" + "</ul></a>"
				html += "</a>"
			}
		html += text.substring(processedCharacters, text.length)
		html = replaceSpecialCharacters(html)
		Article(title, html)
	}

	/**
	  * Inserts HTML links into all article texts where named entities were found.
	  *
	  * @param articles all articles
	  * @param links    all linked named entities found in the articles
	  * @return all articles with HTML as text
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
	  * Uses a trained classifier model to perform NER and NEL on Wikipedia articles.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val articlesWithTermFrequencies = sc.parallelize(input.head.asInstanceOf[RDD[ParsedWikipediaEntry]].take(10000))
		val aliases = input(1).asInstanceOf[RDD[Alias]]
		val numDocuments = articlesWithTermFrequencies.count()

		val defaultDf = calculateDefaultDf(DocumentFrequencyCounter.leastSignificantDocumentFrequency)
		val defaultIdf = calculateIdf(defaultDf, numDocuments)
		val articleTfs = transformArticleTfs(articlesWithTermFrequencies)
		val articleTfidf = calculateTfidf(articleTfs, numDocuments, defaultIdf)

		val aliasPageProbabilities = computeAliasProbabilities(aliases)

		val classifier = modelFunction(sc)
		val articles = articlesWithTermFrequencies.map(article => Article(article.title, article.getText()))

		val linkCandidatesTfs = findLinkCandidates(articles, aliases)
		val linkCandidatesTfidf = calculateTfidf(linkCandidatesTfs, numDocuments, defaultIdf)

		val linkFeatureEntries = CosineContextComparator
			.compareLinksWithArticles(linkCandidatesTfidf, aliasPageProbabilities, articleTfidf)
		val data = linkFeatureEntries
			.map { case (link, featureEntry) => (link, featureEntry.labeledPoint()) }
		val namedEntities = linkNamedEntities(data, classifier)
		val articlesWithLinks = generateArticlesWithLinks(articles, namedEntities)

		List(namedEntities).toAnyRDD() ++ List(articlesWithLinks).toAnyRDD()
	}
}
