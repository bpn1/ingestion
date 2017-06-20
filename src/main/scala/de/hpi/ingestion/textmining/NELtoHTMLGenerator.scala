package de.hpi.ingestion.textmining

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{Article, Link, ParsedWikipediaEntry}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Generates HTML from found links to named entities and their corresponding raw articles.
  */
object NELtoHTMLGenerator extends SparkJob {
	appName = "NEL to HTML Generator"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads Wikipedia entries and found links to named entities from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[ParsedWikipediaEntry](settings("keyspace"), settings("parsedWikiTable"))
		val namedEntities = sc.cassandraTable[Link](settings("keyspace"), settings("namedEntitiesTable"))
		List(articles).toAnyRDD() ++ List(namedEntities).toAnyRDD()
	}

	/**
	  * Saves HTML articles containing links to named entities to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val articlesWithLinks = output.head.asInstanceOf[RDD[Article]]
		articlesWithLinks.saveToCassandra(settings("keyspace"), settings("linkedArticlesTable"))
	}

	/**
	  * Loads the random forest classifier model from the HDFS.
	  *
	  * @param sc SparkContext used for loading from the HDFS
	  * @return random forest classifier model
	  */
	def loadModel(sc: SparkContext): RandomForestModel = {
		RandomForestModel.load(sc, settings("classifierModel"))
	}

	// $COVERAGE-ON$

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
				if(offset.get < processedCharacters) {
					print(s"AAAAA ${offset.get}")
					print(s"AAAAA $processedCharacters")
					print(s"AAAAA $groupedLinks")
					print(s"AAAAA $title")
					print(s"CCCCC $text")
				} else {
					val distinctLinks = groupedLinks.distinct // hotfix against duplicates
					val mainLink = distinctLinks.head
					html += text.substring(processedCharacters, offset.get)
					processedCharacters = offset.get
					val pages = distinctLinks
						.map(_.page)
						.reduce(_ + "\n" + _)
					val wikipediaPage = "https://de.wikipedia.org/wiki/" + mainLink.page.replaceAll(" ", "_")
					html += s"""<a href="$wikipediaPage" title="$pages">"""
					val end = processedCharacters + mainLink.alias.length
					if(processedCharacters > end) {
						print(s"BBBB $processedCharacters")
						print(s"BBBB $end")
						print(s"BBBB ${mainLink.alias.length}")
						print(s"BBBB $mainLink")
					} else {
						html += text.substring(processedCharacters, end)
						processedCharacters += mainLink.alias.length
					}
					//html += "<ul style=\"display: none\">" + "<li>some other link</li>" + "</ul></a>"
					html += "</a>"
				}
			}
		html += text.substring(processedCharacters, text.length)
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
			.join(groupedLinks)
			.map { case (title, (text, linksForArticle)) => (title, text, linksForArticle) }
			.map((generateArticleWithLinks _).tupled)
	}

	/**
	  * Generates HTML from found links to named entities and their corresponding raw articles.
	  *
	  * @param input List of RDDs containing the input data
	  * @param sc    Spark Context used to e.g. broadcast variables
	  * @param args  arguments of the program
	  * @return List of RDDs containing the output data
	  */
	override def run(input: List[RDD[Any]], sc: SparkContext, args: Array[String] = Array[String]()): List[RDD[Any]] = {
		val wikipediaArticles = input.head.asInstanceOf[RDD[ParsedWikipediaEntry]]
		val namedEntities = input(1).asInstanceOf[RDD[Link]]
		val articles = wikipediaArticles.map(article => Article(article.title, article.getText()))
		val articlesWithLinks = generateArticlesWithLinks(articles, namedEntities)
		List(articlesWithLinks).toAnyRDD()
	}
}
