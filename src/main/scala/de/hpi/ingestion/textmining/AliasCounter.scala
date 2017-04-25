package de.hpi.ingestion.textmining

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import de.hpi.ingestion.textmining.models._

/**
  * Counts alias occurrences and merges them into previously extracted aliases with their corresponding pages.
  */
object AliasCounter {
	val keyspace = "wikidumps"
	val inputArticlesTablename = "parsedwikipedia"
	val linksTablename = "wikipedialinks"
	val maximumAliasLength = 1000

	/**
	  * Calculates the probability that an alias is a link.
	  *
	  * @param aliasCounts alias of a given alias
	  * @return percentage of the occurrences as link
	  */
	def probabilityIsLink(aliasCounts: Alias): Double = {
		aliasCounts.linkoccurrences.toDouble / aliasCounts.totaloccurrences
	}

	/**
	  * Extracts list of link and general alias occurrences for an article.
	  *
	  * @param entry article from which the aliases will be extracted
	  * @return list of aliases each with an occurrence set to 1
	  */
	def extractAliasList(entry: ParsedWikipediaEntry): List[Alias] = {
		val linkSet = entry.allLinks()
			.map(_.alias)
			.toSet

		val links = linkSet
			.toList
			.map(Alias(_, Map(), 1, 1))

		val aliases = entry.foundaliases
			.toSet
			.filterNot(linkSet)
			.toList
			.map(Alias(_, Map(), 0, 1))

		links ++ aliases
	}

	/**
	  * Reduces two AliasCounters of the same alias.
	  *
	  * @param alias1 first Alias with the same alias as alias2
	  * @param alias2 second Alias with the same alias as alias1
	  * @return alias with summed link and total occurrences
	  */
	def aliasReduction(alias1: Alias, alias2: Alias): Alias = {
		Alias(
			alias1.alias,
			alias1.pages ++ alias2.pages,
			alias1.linkoccurrences + alias2.linkoccurrences,
			alias1.totaloccurrences + alias2.totaloccurrences)
	}

	/**
	  * Counts link and total occurrences of link aliases in Wikipedia articles.
	  *
	  * @param articles RDD containing parsed wikipedia articles
	  * @return RDD containing alias counts for links and total occurrences
	  */
	def countAliases(articles: RDD[ParsedWikipediaEntry]): RDD[Alias] = {
		articles
			.flatMap(extractAliasList)
			.map(alias => (alias.alias, alias))
			.reduceByKey(aliasReduction)
			.map(_._2)
			.filter(_.alias.nonEmpty)
	}

	/**
	  * Counts alias occurrences and merges them into previously extracted aliases with their corresponding pages.
	  *
	  * @param articles parsed Wikipedia articles
	  * @param links    articles with their corresponding pages
	  * @return merged links and alias counts
	  */
	def run(articles: RDD[ParsedWikipediaEntry], links: RDD[Alias]): RDD[(String, Int, Int)] = {
		countAliases(articles)
			.cache
			.filter(_.alias.length <= maximumAliasLength)
			.map(entry => (entry.alias, entry.linkoccurrences, entry.totaloccurrences))
	}

	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("Alias Counter")
		val sc = new SparkContext(conf)

		val articles = sc.cassandraTable[ParsedWikipediaEntry](keyspace, inputArticlesTablename)
		val links = sc.cassandraTable[Alias](keyspace, linksTablename)
		run(articles, links)
			.saveToCassandra(keyspace, linksTablename, SomeColumns("alias", "linkoccurrences", "totaloccurrences"))

		sc.stop()
	}
}
