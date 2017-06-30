package de.hpi.ingestion.textmining.nel

import de.hpi.ingestion.framework.SparkJob
import de.hpi.ingestion.implicits.CollectionImplicits._
import de.hpi.ingestion.textmining.models.{Article, TrieAliasArticle}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._

/**
  * Generates HTML from found links to named entities and their corresponding raw articles.
  */
object HtmlGenerator extends SparkJob {
	appName = "HTML Generator"
	configFile = "textmining.xml"

	// $COVERAGE-OFF$
	/**
	  * Loads articles with their found links to named entities from the Cassandra.
	  *
	  * @param sc   Spark Context used to load the RDDs
	  * @param args arguments of the program
	  * @return List of RDDs containing the data processed in the job.
	  */
	override def load(sc: SparkContext, args: Array[String]): List[RDD[Any]] = {
		val articles = sc.cassandraTable[TrieAliasArticle](settings("keyspace"), settings("NELTable"))
		List(articles).toAnyRDD()
	}

	/**
	  * Saves HTML articles containing links to named entities to the Cassandra.
	  *
	  * @param output List of RDDs containing the output of the job
	  * @param sc     Spark Context used to connect to the Cassandra or the HDFS
	  * @param args   arguments of the program
	  */
	override def save(output: List[RDD[Any]], sc: SparkContext, args: Array[String]): Unit = {
		val htmlArticles = output.fromAnyRDD[Article]().head
		htmlArticles.saveToCassandra(settings("keyspace"), settings("linkedArticlesTable"))
	}
	// $COVERAGE-ON$

	/**
	  * Inserts HTML links into an article text where named entities were found.
	  *
	  * @param article article with its found links to named entities
	  * @return HTML containing links
	  */
	def generateArticleWithLinks(article: TrieAliasArticle): Article = {
		val title = article.article
		var html = s"<h1>$title</h1>\n"
		val text = article.getText
		var processedCharacters = 0
		article.foundentities
			.groupBy(_.offset)
			.toList
			.sortBy(_._1)
			.foreach { case (offset, groupedLinks) =>
				val distinctLinks = groupedLinks.distinct // hotfix against duplicates
				val mainLink = distinctLinks.head
				html += text.substring(processedCharacters, offset.get)
				processedCharacters = offset.get
				val pages = distinctLinks
					.map(_.page)
					.reduce(_ + "\n" + _)
				val page = "https://de.wikipedia.org/wiki/" + mainLink.page.replaceAll(" ", "_")
				html += s"""<a href="$page" title="$pages">"""
				val end = processedCharacters + mainLink.alias.length
				html += text.substring(processedCharacters, end)
				processedCharacters += mainLink.alias.length
				html += "</a>"
			}
		html += text.substring(processedCharacters, text.length)
		Article(title, html)
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
		val articlesWithLinks = input.fromAnyRDD[TrieAliasArticle]().head
		val htmlArticles = articlesWithLinks.map(generateArticleWithLinks)
		List(htmlArticles).toAnyRDD()
	}
}
